from __future__ import annotations

import os
import re
import time
import queue
import threading
import datetime
from dataclasses import dataclass
from typing import Dict, Optional, Literal, Any, Tuple, List
from concurrent.futures import Future, wait

from playwright.sync_api import sync_playwright, Page


Side = Literal["long", "short"]
Action = Literal["open", "close"]
OrderType = Literal["market", "limit"]
MarginMode = Literal["Cross", "Isolated"]


@dataclass
class MarketSnapshot:
    symbol: str
    price: float
    ask_price: float
    ask_qty: float
    bid_price: float
    bid_qty: float
    source: str
    ts: float


@dataclass
class OrderResult:
    symbol: str
    ok: bool
    error: Optional[str] = None
    screenshot: Optional[str] = None


@dataclass
class UIHealthResult:
    symbol: str
    ok: bool
    error: Optional[str] = None


@dataclass
class _Req:
    op: str
    kwargs: Dict[str, Any]
    fut: Future


class _SymbolWorker(threading.Thread):
    """
    每個 symbol 一個 worker thread：
    - thread 內部持有自己的 playwright/browser/context/page（避免跨 thread 操作）
    - page 常駐，不會每次下單都重開
    """
    BASE_URL = "https://www.kcex.com/futures/exchange"
    LOGIN_URL = "https://www.kcex.com/login"

    def __init__(
        self,
        symbol: str,
        storage_state_path: str,
        headless: bool,
        viewport: Tuple[int, int],
        default_wait_ms: int,
        keep_alive_sec: float,
    ):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.storage_state_path = storage_state_path
        self.headless = headless
        self.viewport = viewport
        self.default_wait_ms = default_wait_ms
        self.keep_alive_sec = keep_alive_sec

        self.q: "queue.Queue[_Req]" = queue.Queue()
        self._stop_evt = threading.Event()

        # playwright runtime objects (thread-owned)
        self._p = None
        self._browser = None
        self._context = None
        self.page: Optional[Page] = None

        self._ready_evt = threading.Event()
        self._startup_error: Optional[str] = None

    # ---- helpers ----
    @staticmethod
    def add_stealth_script(page: Page) -> None:
        page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            """
        )

    @staticmethod
    def parse_number(text: str) -> float:
        if not text:
            return 0.0
        try:
            t = text.replace(",", "").strip()
            mul = 1.0
            if t.endswith("K"):
                mul = 1_000.0
                t = t[:-1]
            elif t.endswith("M"):
                mul = 1_000_000.0
                t = t[:-1]
            return float(t) * mul
        except:
            return 0.0

    def _extract_price_from_title(self, title: str) -> float:
        # 常見 title: "5,012.3 PAXG/USDT ..."
        if not title or "USDT" not in title:
            return 0.0
        m = re.search(r"([\d,]+(?:\.\d+)?)\s+[A-Z0-9]+/USDT", title)
        if m:
            return self.parse_number(m.group(1))
        parts = title.split(" ")
        return self.parse_number(parts[0]) if parts else 0.0

    def _goto_symbol(self) -> None:
        assert self.page is not None
        self.page.goto(f"{self.BASE_URL}/{self.symbol}_USDT")
        self.page.wait_for_timeout(self.default_wait_ms)

    def _keep_alive(self) -> None:
        # 輕量操作避免 session/頁面掛掉（也避免長時間無操作導致 DOM 找不到）
        if self.page and not self.page.is_closed():
            try:
                _ = self.page.title()
            except:
                pass

    # ---- market data ----
    def _get_snapshot(self) -> MarketSnapshot:
        assert self.page is not None

        price = 0.0
        source = "unknown"
        try:
            title = self.page.title()
            price = self._extract_price_from_title(title)
            if price > 0:
                source = "Title"
        except:
            pass

        # DOM fallback
        if price == 0:
            try:
                price_el = self.page.locator("div[class*='market_bigPrice']").first
                if price_el.is_visible():
                    price = self.parse_number(price_el.inner_text())
                    if price > 0:
                        source = "DOM (bigPrice)"
            except:
                pass

        ask_p = ask_q = bid_p = bid_q = 0.0

        # orderbook (best ask=asks last, best bid=bids first) —— 跟你原本 heartbeat/orderbook 的 selector 一致方向
        try:
            asks = self.page.locator("div[class*='market_asksWrapper']")
            if asks.is_visible():
                row = asks.locator("div[class*='market_tableRow']").last
                if row.is_visible():
                    p_txt = row.locator("div[class*='market_price']").inner_text().strip()
                    q_txt = row.locator("div[class*='market_vol']").inner_text().strip()
                    ask_p = self.parse_number(p_txt)
                    ask_q = self.parse_number(q_txt)
        except:
            pass

        try:
            bids = self.page.locator("div[class*='market_bidsWrapper']")
            if bids.is_visible():
                row = bids.locator("div[class*='market_tableRow']").first
                if row.is_visible():
                    p_txt = row.locator("div[class*='market_price']").inner_text().strip()
                    q_txt = row.locator("div[class*='market_vol']").inner_text().strip()
                    bid_p = self.parse_number(p_txt)
                    bid_q = self.parse_number(q_txt)
        except:
            pass

        return MarketSnapshot(
            symbol=self.symbol,
            price=price,
            ask_price=ask_p,
            ask_qty=ask_q,
            bid_price=bid_p,
            bid_qty=bid_q,
            source=source,
            ts=time.time(),
        )
    
    # ---- screenshot ----
    def _capture_page(self, path: Optional[str] = None, full_page: bool = False) -> Optional[str]:
        assert self.page is not None
        page = self.page

        out_path = path or f"{self.symbol}_latest.png"
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        try:
            page.screenshot(path=out_path, full_page=full_page)
            return out_path
        except:
            return None

    def _dismiss_blocking_modal(self) -> bool:
        assert self.page is not None
        page = self.page
        dismissed = False

        try:
            modal = page.locator("div.ant-modal-wrap").filter(visible=True).last
            if modal.count() == 0:
                return False

            close_selectors = [
                "button.ant-modal-close",
                ".ant-modal-close",
                ".ant-modal-close-x",
                "button[aria-label='Close']",
            ]

            for sel in close_selectors:
                try:
                    btn = modal.locator(sel).filter(visible=True).first
                    if btn.count() > 0:
                        btn.click(timeout=1500)
                        time.sleep(0.2)
                        dismissed = True
                        break
                except:
                    pass

            if not dismissed:
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.2)
                    dismissed = True
                except:
                    pass
        except:
            pass

        return dismissed

    def _health_check_ui(
        self,
        action: Action = "open",
        order_type: OrderType = "market",
        side: Side = "long",
    ) -> UIHealthResult:
        """
        快速 UI 健康檢查（下單前用）：
        - 會「真的點」Open/Close、Market/Limit 來切到正確分頁（安全，因為不會送單）
        - qty / submit 使用 trial click，確保可操作但不真的送出
        - 若有 blocking modal 會嘗試關閉；仍存在就判定失敗
        """
        assert self.page is not None
        page = self.page

        try:
            trade_panel = page.locator("#kcex-web-inspection-futures-exchange-orderForm")
            trade_panel.wait_for(state="visible", timeout=5000)

            # 先嘗試關掉已知的 blocking modal（像你截圖那種 ant-modal）
            self._dismiss_blocking_modal()

            blocking_modal = page.locator("div.ant-modal-wrap").filter(visible=True)
            if blocking_modal.count() > 0:
                return UIHealthResult(
                    symbol=self.symbol,
                    ok=False,
                    error="blocking modal still visible before order",
                )

            # 1) open / close tab：必須真的 click 才會切換分頁（trial 不會改變 UI 狀態）
            action_tab = trade_panel.locator("div[class*='handle_tabs'] span").filter(
                has_text=action.capitalize()
            ).first
            action_tab.wait_for(state="visible", timeout=2000)
            action_tab.click(timeout=2000)

            # 2) market / limit tab：同樣用真的 click 切換（安全）
            if order_type == "market":
                order_tab = trade_panel.get_by_text("Market", exact=True).filter(visible=True).first
            else:
                order_tab = trade_panel.get_by_text("Limit", exact=True).filter(visible=True).first
            order_tab.wait_for(state="visible", timeout=2000)
            order_tab.click(timeout=2000)

            # 3) qty input（只檢查可操作，不輸入、不送出）
            visible_inputs = trade_panel.locator(".ant-input").filter(visible=True)
            need_inputs = 2 if order_type == "limit" else 1

            # 等到至少 need_inputs 個 input 可見（取代 time.sleep）
            try:
                visible_inputs.nth(need_inputs - 1).wait_for(state="visible", timeout=2000)
            except Exception:
                pass

            if visible_inputs.count() < need_inputs:
                return UIHealthResult(
                    symbol=self.symbol,
                    ok=False,
                    error=f"visible input count not enough: need={need_inputs}",
                )

            qty_input = visible_inputs.nth(1 if order_type == "limit" else 0)
            qty_input.click(trial=True, timeout=1500)

            # 4) submit button（trial click：確保不被彈窗/overlay 擋住）
            if action == "open":
                btn_text = "Open Long" if side == "long" else "Open Short"
            else:
                btn_text = "Close Long" if side == "long" else "Close Short"

            submit_btn = trade_panel.locator("button").filter(has_text=btn_text, visible=True).first
            submit_btn.wait_for(state="visible", timeout=3000)
            submit_btn.click(trial=True, timeout=1500)

            return UIHealthResult(symbol=self.symbol, ok=True)

        except Exception as e:
            return UIHealthResult(symbol=self.symbol, ok=False, error=str(e))

    def _place_order(
        self,
        action: Action,
        order_type: OrderType,
        amount_usdt: str,
        side: Side,
        target_price: str,
        margin_mode: MarginMode,
        leverage: str,
        barrier: Optional[threading.Barrier],
        barrier_timeout_sec: float,
        take_screenshot: bool,
    ) -> OrderResult:
        assert self.page is not None
        page = self.page

        def shot(name: str) -> Optional[str]:
            if not take_screenshot:
                return None
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            path = rf"trading capture\{self.symbol}_{name}_{ts}.png"
            try:
                page.screenshot(path=path)
                return path
            except:
                return None

        try:
            trade_panel = page.locator("#kcex-web-inspection-futures-exchange-orderForm")
            trade_panel.wait_for(state="visible", timeout=15000)

            # 1) open/close tab
            action_tab = trade_panel.locator("div[class*='handle_tabs'] span").filter(has_text=action.capitalize()).first
            action_tab.wait_for(state="visible", timeout=5000)
            action_tab.click()

            # 2) market/limit tab
            if order_type == "market":
                order_tab = trade_panel.get_by_text("Market", exact=True).filter(visible=True).first
            else:
                order_tab = trade_panel.get_by_text("Limit", exact=True).filter(visible=True).first
            order_tab.wait_for(state="visible", timeout=5000)
            order_tab.click()

            # 3) unit => USDT
            unit_selector = trade_panel.locator("div[class*='UnitSelect_wrapper']").filter(visible=True).first
            unit_selector.wait_for(state="visible", timeout=5000)
            current_unit = (unit_selector.inner_text() or "").strip()
            if "USDT" not in current_unit:
                unit_selector.click()
                dlg = page.get_by_role("dialog").last
                dlg.wait_for(state="visible", timeout=5000)
                dlg.get_by_text("USDT", exact=True).first.click()
                dlg.get_by_role("button", name="Confirm").click()

                # 等 unit 文字真的更新成 USDT（取代 time.sleep）
                try:
                    page.wait_for_function(
                        """() => {
                            const root = document.querySelector("#kcex-web-inspection-futures-exchange-orderForm");
                            if (!root) return false;
                            const els = Array.from(root.querySelectorAll("div[class*='UnitSelect_wrapper']")).filter(e => e.offsetParent !== null);
                            if (els.length < 1) return false;
                            return (els[0].innerText || "").includes("USDT");
                        }""",
                        timeout=3000,
                    )
                except Exception:
                    pass

            # 4) margin/leverage
            leverage_btns = trade_panel.locator("span[class*='LeverageEdit_leverageText']").filter(visible=True)
            margin_btn = leverage_btns.nth(0)
            lev_btn = leverage_btns.nth(1)

            margin_btn.wait_for(state="visible", timeout=5000)
            if margin_mode.lower() not in (margin_btn.inner_text() or "").lower():
                margin_btn.click()
                dlg = page.get_by_role("dialog")
                dlg.get_by_text(margin_mode, exact=True).click()
                dlg.get_by_role("button", name="Confirm").click()

                # 等 dialog 關閉 + margin 文字更新（取代 time.sleep）
                try:
                    dlg.wait_for(state="hidden", timeout=5000)
                except Exception:
                    pass
                try:
                    page.wait_for_function(
                        """(target) => {
                            const root = document.querySelector("#kcex-web-inspection-futures-exchange-orderForm");
                            if (!root) return false;
                            const els = Array.from(root.querySelectorAll("span[class*='LeverageEdit_leverageText']")).filter(e => e.offsetParent !== null);
                            if (els.length < 1) return false;
                            return (els[0].innerText || "").toLowerCase().includes(String(target).toLowerCase());
                        }""",
                        margin_mode,
                        timeout=3000,
                    )
                except Exception:
                    pass

            lev_btn.wait_for(state="visible", timeout=5000)
            lev_text = (lev_btn.inner_text() or "").strip()
            m = re.search(r"[\d\.]+", lev_text)
            current_lev = float(m.group(0)) if m else 0.0
            target_lev = float(leverage)
            if current_lev != target_lev:
                lev_btn.click()
                lev_input = page.locator("input[class*='LeverageProgress_leverageInput']")
                lev_input.fill(str(leverage))
                page.get_by_role("dialog").get_by_role("button", name="Confirm").click()

                # 等 dialog 關閉 + leverage 文字更新（取代 time.sleep）
                try:
                    page.get_by_role("dialog").wait_for(state="hidden", timeout=5000)
                except Exception:
                    pass
                try:
                    page.wait_for_function(
                        """(lev) => {
                            const root = document.querySelector("#kcex-web-inspection-futures-exchange-orderForm");
                            if (!root) return false;
                            const els = Array.from(root.querySelectorAll("span[class*='LeverageEdit_leverageText']")).filter(e => e.offsetParent !== null);
                            if (els.length < 2) return false;
                            const t = (els[1].innerText || "").trim();
                            const m = t.match(/[\d\.]+/);
                            if (!m) return false;
                            return Math.abs(parseFloat(m[0]) - parseFloat(lev)) < 1e-9;
                        }""",
                        str(leverage),
                        timeout=3000,
                    )
                except Exception:
                    pass

            # 5) fill inputs
            visible_inputs = trade_panel.locator(".ant-input").filter(visible=True)

            # 等到需要的 input 數量可見（取代 tab 切換後的 sleep）
            need_inputs = 2 if order_type == "limit" else 1
            try:
                visible_inputs.nth(need_inputs - 1).wait_for(state="visible", timeout=3000)
            except Exception:
                pass

            if order_type == "limit":
                price_input = visible_inputs.nth(0)
                price_input.click()
                price_input.press("Control+A")
                price_input.press("Backspace")
                price_input.fill(str(target_price))

                qty_input = visible_inputs.nth(1)
                qty_input.click()
                qty_input.press("Control+A")
                qty_input.press("Backspace")
                qty_input.fill(str(amount_usdt))
            else:
                qty_input = visible_inputs.nth(0)
                qty_input.click()
                qty_input.press("Control+A")
                qty_input.press("Backspace")
                qty_input.fill(str(amount_usdt))

            # 6) 同步點擊（核心：在 submit 前 barrier 對齊）
            if action == "open":
                btn_text = "Open Long" if side == "long" else "Open Short"
            else:
                btn_text = "Close Long" if side == "long" else "Close Short"

            submit_btn = trade_panel.locator("button").filter(has_text=btn_text, visible=True).first
            submit_btn.wait_for(state="visible", timeout=5000)

            if barrier is not None:
                try:
                    barrier.wait(timeout=barrier_timeout_sec)
                except threading.BrokenBarrierError:
                    return OrderResult(symbol=self.symbol, ok=False, error="Barrier broken/timeout", screenshot=shot("BARRIER_FAIL"))

            submit_btn.click()

            # 7) 二次確認彈窗
            try:
                confirm_btn = page.get_by_role("button", name="Confirm").last
                if confirm_btn.is_visible(timeout=2000):
                    confirm_btn.click()
                    # 等 dialog 關閉，避免立刻截圖截到彈窗
                    try:
                        page.get_by_role("dialog").last.wait_for(state="hidden", timeout=5000)
                    except Exception:
                        pass
            except:
                pass

            # 8) 等 UI 反應（取代 time.sleep）：toast 出現或 submit 不再 loading/disabled
            try:
                page.wait_for_function(
                    """(btnText) => {
                        const toast = document.querySelector(".ant-message") || document.querySelector(".ant-notification");
                        if (toast && toast.offsetParent !== null) return true;

                        const root = document.querySelector("#kcex-web-inspection-futures-exchange-orderForm");
                        if (!root) return false;
                        const btn = Array.from(root.querySelectorAll("button")).find(b => (b.innerText || "").includes(btnText) && b.offsetParent !== null);
                        if (!btn) return false;
                        const cls = btn.className || "";
                        const disabled = !!btn.disabled || btn.getAttribute("disabled") !== null;
                        const loading = cls.includes("ant-btn-loading");
                        return !disabled && !loading;
                    }""",
                    btn_text,
                    timeout=1500,
                )
            except Exception:
                pass

            return OrderResult(symbol=self.symbol, ok=True, screenshot=shot("OK"))

        except Exception as e:
            # 如果其中一腿在到 barrier 前就掛了，最好 abort barrier 釋放另一腿避免卡死
            try:
                if barrier is not None:
                    barrier.abort()
            except:
                pass
            return OrderResult(symbol=self.symbol, ok=False, error=str(e), screenshot=shot("ERROR"))

    # ---- thread main ----
    def run(self) -> None:
        try:
            if not os.path.exists(self.storage_state_path):
                raise FileNotFoundError(f"missing {self.storage_state_path}; please run save_auth() first")

            self._p = sync_playwright().start()
            
            # 1. 加入隱藏自動化特徵的啟動參數
            self._browser = self._p.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"]
            )
            
            # 2. 強制寫入一個真實的 Windows Chrome User-Agent
            self._context = self._browser.new_context(
                storage_state=self.storage_state_path,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            
            self.page = self._context.new_page()
            self.page.set_viewport_size({"width": self.viewport[0], "height": self.viewport[1]})
            self.add_stealth_script(self.page)

            self._goto_symbol()
            self._ready_evt.set()

        except Exception as e:
            self._startup_error = str(e)
            self._ready_evt.set()
            return

        # main loop
        while not self._stop_evt.is_set():
            try:
                req = self.q.get(timeout=self.keep_alive_sec)
            except queue.Empty:
                self._keep_alive()
                continue

            if req.op == "shutdown":
                req.fut.set_result(True)
                break

            try:
                if req.op == "get_snapshot":
                    req.fut.set_result(self._get_snapshot())
                elif req.op == "health_check_ui":
                    req.fut.set_result(self._health_check_ui(**req.kwargs))
                elif req.op == "place_order":
                    req.fut.set_result(self._place_order(**req.kwargs))
                elif req.op == "capture_page":
                    req.fut.set_result(self._capture_page(**req.kwargs))
                else:
                    req.fut.set_exception(RuntimeError(f"unknown op: {req.op}"))

            except Exception as e:
                req.fut.set_exception(e)

        # cleanup
        try:
            if self.page and not self.page.is_closed():
                self.page.close()
        except:
            pass
        try:
            if self._context:
                self._context.close()
        except:
            pass
        try:
            if self._browser:
                self._browser.close()
        except:
            pass
        try:
            if self._p:
                self._p.stop()
        except:
            pass

    def wait_ready(self, timeout_sec: float = 30.0) -> None:
        ok = self._ready_evt.wait(timeout=timeout_sec)
        if not ok:
            raise TimeoutError(f"[{self.symbol}] worker not ready (timeout)")
        if self._startup_error:
            raise RuntimeError(f"[{self.symbol}] startup failed: {self._startup_error}")

    def submit(self, op: str, **kwargs) -> Future:
        fut: Future = Future()
        self.q.put(_Req(op=op, kwargs=kwargs, fut=fut))
        return fut

    def shutdown(self) -> None:
        fut = self.submit("shutdown")
        self._stop_evt.set()
        try:
            fut.result(timeout=5)
        except:
            pass


class KCEXTool:
    """
    統合工具（你策略端只要用這個 class）：
    - start(): 啟動兩個常駐 worker（PAXG / XAUT）
    - get_multi_snapshot(): 多 thread 同時抓兩邊價格
    - two_legs_trade(): 多 thread 幾乎同時點擊下單（Barrier 同步）
    """
    @staticmethod
    def save_auth(out_path: str = "auth.json", headless: bool = False) -> None:
        # 你原本 setup.py 的保存登入狀態邏輯
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context()
            page = context.new_page()
            page.goto(_SymbolWorker.LOGIN_URL)

            print("請在瀏覽器中手動登入 (含 2FA)...")
            print("登入成功並跳轉至首頁後，請按 Enter 繼續...")
            input()

            context.storage_state(path=out_path)
            print(f"登入狀態已保存至 {out_path}")
            browser.close()

    def __init__(
        self,
        storage_state_path: str = "auth.json",
        headless: bool = False,
        viewport: Tuple[int, int] = (1920, 1080),
        default_wait_ms: int = 5000,
        keep_alive_sec: float = 15.0,
    ):
        self.storage_state_path = storage_state_path
        self.headless = headless
        self.viewport = viewport
        self.default_wait_ms = default_wait_ms
        self.keep_alive_sec = keep_alive_sec

        self.workers: Dict[str, _SymbolWorker] = {}

    @staticmethod
    def _norm_symbol(symbol: str) -> str:
        s = symbol.upper().strip()
        s = s.replace("/", "").replace("_USDT", "").replace("USDT", "")
        return s

    def start(self, symbols: List[str] = ["PAXG", "XAUT"]) -> "KCEXTool":
        if not os.path.exists(self.storage_state_path):
            raise FileNotFoundError(f"找不到 {self.storage_state_path}，請先跑 KCEXTool.save_auth()")

        for s in symbols:
            sym = self._norm_symbol(s)
            if sym in self.workers:
                continue
            w = _SymbolWorker(
                symbol=sym,
                storage_state_path=self.storage_state_path,
                headless=self.headless,
                viewport=self.viewport,
                default_wait_ms=self.default_wait_ms,
                keep_alive_sec=self.keep_alive_sec,
            )
            self.workers[sym] = w
            w.start()

        # 等待 ready
        for sym, w in self.workers.items():
            w.wait_ready(timeout_sec=45)

        return self

    def stop(self) -> None:
        for sym, w in list(self.workers.items()):
            try:
                w.shutdown()
            except:
                pass
        self.workers.clear()

    def refresh(self, symbols: Optional[List[str]] = None) -> "KCEXTool":
 
        if symbols is None:
            if self.workers:
                symbols = list(self.workers.keys())
            else:
                symbols = ["PAXG", "XAUT"]

        # 先停掉舊的
        self.stop()

        # 小睡一下，避免 OS/Playwright 還在收尾導致偶發啟動失敗
        time.sleep(0.3)

        # 再重啟
        return self.start(symbols)

    def __enter__(self) -> "KCEXTool":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    # ---- market ----
    def get_snapshot(self, symbol: str) -> MarketSnapshot:
        sym = self._norm_symbol(symbol)
        w = self.workers[sym]
        fut = w.submit("get_snapshot")
        return fut.result(timeout=10)

    def get_multi_snapshot(self, symbols: List[str]) -> Dict[str, MarketSnapshot]:
        futs: Dict[str, Future] = {}
        for s in symbols:
            sym = self._norm_symbol(s)
            futs[sym] = self.workers[sym].submit("get_snapshot")
        wait(list(futs.values()), timeout=10)
        return {sym: fut.result() for sym, fut in futs.items()}

    # ---- orders ----
    def health_check_ui(
        self,
        symbol: str,
        action: Action = "open",
        order_type: OrderType = "market",
        side: Side = "long",
    ) -> UIHealthResult:
        sym = self._norm_symbol(symbol)
        if sym not in self.workers:
            self.start([sym])

        w = self.workers[sym]
        fut = w.submit(
            "health_check_ui",
            action=action,
            order_type=order_type,
            side=side,
        )
        return fut.result(timeout=20)

    @staticmethod
    def _health_fail_to_order_result(health: UIHealthResult) -> OrderResult:
        msg = health.error or "unknown ui health check error"
        return OrderResult(
            symbol=health.symbol,
            ok=False,
            error=f"Pre-trade UI health check failed: {msg}",
        )

    def _active_symbols_or(self, fallback: List[str]) -> List[str]:
        if self.workers:
            return list(self.workers.keys())
        return fallback

    def _health_check_pair_for_two_legs(
        self,
        strategy: Literal["long_paxg", "short_paxg"],
        action: Action,
        order_type: OrderType,
    ) -> Dict[str, UIHealthResult]:
        if strategy == "long_paxg":
            paxg_side: Side = "long"
            xaut_side: Side = "short"
        else:
            paxg_side = "short"
            xaut_side = "long"

        f1 = self.workers["PAXG"].submit(
            "health_check_ui",
            action=action,
            order_type=order_type,
            side=paxg_side,
        )
        f2 = self.workers["XAUT"].submit(
            "health_check_ui",
            action=action,
            order_type=order_type,
            side=xaut_side,
        )

        wait([f1, f2], timeout=20)
        return {
            "PAXG": f1.result(),
            "XAUT": f2.result(),
        }

    def place_order(
        self,
        symbol: str,
        action: Action,
        order_type: OrderType,
        amount_usdt: str,
        side: Side,
        target_price: str = "0",
        margin_mode: MarginMode = "Cross",
        leverage: str = "20",
        take_screenshot: bool = True,
    ) -> OrderResult:
        sym = self._norm_symbol(symbol)
        if sym not in self.workers:
            self.start([sym])

        health = self.health_check_ui(
            sym,
            action=action,
            order_type=order_type,
            side=side,
        )
        if not health.ok:
            self.refresh(self._active_symbols_or([sym]))
            health = self.health_check_ui(
                sym,
                action=action,
                order_type=order_type,
                side=side,
            )
            if not health.ok:
                return self._health_fail_to_order_result(health)

        w = self.workers[sym]
        fut = w.submit(
            "place_order",
            action=action,
            order_type=order_type,
            amount_usdt=amount_usdt,
            side=side,
            target_price=target_price,
            margin_mode=margin_mode,
            leverage=leverage,
            barrier=None,
            barrier_timeout_sec=0.0,
            take_screenshot=take_screenshot,
        )
        return fut.result(timeout=60)

    def capture_page(
        self,
        symbol: str,
        path: Optional[str] = None,
        full_page: bool = False,
    ) -> Optional[str]:
        sym = self._norm_symbol(symbol)
        w = self.workers[sym]
        fut = w.submit(
            "capture_page",
            path=path,
            full_page=full_page,
        )
        return fut.result(timeout=30)

    def capture_pages(
        self,
        paths: Dict[str, str],
        full_page: bool = False,
    ) -> Dict[str, Optional[str]]:
        futs: Dict[str, Future] = {}
        for symbol, path in paths.items():
            sym = self._norm_symbol(symbol)
            futs[sym] = self.workers[sym].submit(
                "capture_page",
                path=path,
                full_page=full_page,
            )
        wait(list(futs.values()), timeout=30)
        return {sym: fut.result() for sym, fut in futs.items()}

    def two_legs_trade(
        self,
        strategy: Literal["long_paxg", "short_paxg"],
        action: Action,
        order_type: OrderType,
        amount_usdt: str,
        margin_mode: MarginMode = "Cross",
        leverage: str = "20",
        paxg_price: str = "0",
        xaut_price: str = "0",
        barrier_timeout_sec: float = 8.0,
        take_screenshot: bool = True,
    ) -> Dict[str, OrderResult]:
        """
        保留你要的「多 thread 同時點擊」：
        - 兩個 worker thread 各自操作自己的 page
        - 用 Barrier 在 submit 前同步
        - 但在真正下單前，先做一次快速 UI health check
        """
        if "PAXG" not in self.workers or "XAUT" not in self.workers:
            self.start(["PAXG", "XAUT"])

        health = self._health_check_pair_for_two_legs(
            strategy=strategy,
            action=action,
            order_type=order_type,
        )
        if not (health["PAXG"].ok and health["XAUT"].ok):
            self.refresh(["PAXG", "XAUT"])
            health = self._health_check_pair_for_two_legs(
                strategy=strategy,
                action=action,
                order_type=order_type,
            )
            if not (health["PAXG"].ok and health["XAUT"].ok):
                return {
                    "PAXG": self._health_fail_to_order_result(health["PAXG"]),
                    "XAUT": self._health_fail_to_order_result(health["XAUT"]),
                }

        barrier = threading.Barrier(2)

        if strategy == "long_paxg":
            paxg_side: Side = "long"
            xaut_side: Side = "short"
        else:
            paxg_side = "short"
            xaut_side = "long"

        # 同步點擊：兩腿都拿同一個 barrier
        f1 = self.workers["PAXG"].submit(
            "place_order",
            action=action,
            order_type=order_type,
            amount_usdt=amount_usdt,
            side=paxg_side,
            target_price=paxg_price if order_type == "limit" else "0",
            margin_mode=margin_mode,
            leverage=leverage,
            barrier=barrier,
            barrier_timeout_sec=barrier_timeout_sec,
            take_screenshot=take_screenshot,
        )
        f2 = self.workers["XAUT"].submit(
            "place_order",
            action=action,
            order_type=order_type,
            amount_usdt=amount_usdt,
            side=xaut_side,
            target_price=xaut_price if order_type == "limit" else "0",
            margin_mode=margin_mode,
            leverage=leverage,
            barrier=barrier,
            barrier_timeout_sec=barrier_timeout_sec,
            take_screenshot=take_screenshot,
        )

        wait([f1, f2], timeout=90)
        return {
            "PAXG": f1.result(),
            "XAUT": f2.result(),
        }


if __name__ == "__main__":
    KCEXTool.save_auth("auth.json", headless=False)