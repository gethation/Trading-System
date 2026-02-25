from playwright.sync_api import sync_playwright, Page

def get_wallet_value(page: Page, label_name: str) -> float:
    """
    根據 HTML 結構，精準定位並獲取錢包數值
    """
    try:
        # 1. 尋找所有帶有 'ant-row-space-between' 的橫列 (避開動態 hash class)
        # 2. 過濾出包含目標文字 (如 "Wallet Balance") 的那一列
        row = page.locator("div.ant-row-space-between").filter(
            has=page.get_by_text(label_name, exact=True)
        ).first
        
        # 3. 在該列中，尋找帶有 'hasUnitDir' 的目標數值區塊
        val_element = row.locator("div.hasUnitDir").first
        
        # 4. 等待元素出現並取得文字 (例如: "8.22 USDT")
        val_element.wait_for(state="visible", timeout=5000)
        val_text = val_element.inner_text()
        
        # 5. 清理字串並轉為浮點數
        clean_text = val_text.replace("USDT", "").replace(",", "").strip()
        return float(clean_text)
        
    except Exception as e:
        print(f"[-] 獲取 {label_name} 失敗: {e}")
        return 0.0

def main():
    storage_state_path = r"Trading-System\auth.json" 
    target_url = "https://www.kcex.com/futures/exchange/PAXG_USDT"

    print("啟動瀏覽器測試獲取 Wallet 資訊...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(storage_state=storage_state_path)
        page = context.new_page()
        
        # 注入防偵測腳本
        page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            """
        )
        
        page.goto(target_url)
        print("等待頁面載入並尋找資產區塊...")
        
        # 確保整個資產細節區塊已經載入在畫面上
        page.locator("div[class*='assets_assetDetail']").first.wait_for(state="visible", timeout=15000)
        
        # 抓取數值
        wallet_balance = get_wallet_value(page, "Wallet Balance")
        total_equity = get_wallet_value(page, "Total Equity")
        available_margin = get_wallet_value(page, "Available Margin")
        
        print("\n=== 💰 錢包資訊擷取結果 ===")
        print(f"Wallet Balance   : {wallet_balance} USDT")
        print(f"Total Equity     : {total_equity} USDT")
        print(f"Available Margin : {available_margin} USDT")
        print("============================\n")
        
        browser.close()

if __name__ == "__main__":
    main()