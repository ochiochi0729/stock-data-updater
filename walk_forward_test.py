# -------------------------------------------------
        # 新規銘柄の購入（エントリー） - 虫眼鏡モード
        # -------------------------------------------------
        valid_candidates = []
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue 
            df = dict_dfs[ticker]
            
            if current_date not in df.index:
                continue
                
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx >= 0:
                valid_candidates.append((ticker, df.iloc[prev_idx].get('Volume', 0)))
        
        valid_candidates.sort(key=lambda x: x[1], reverse=True) 

        for ticker, _ in valid_candidates:
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx < 0: continue
                
            prev_close = float(df.iloc[prev_idx].get('Close', np.nan))
            today_data = df.loc[current_date]
            
            sma25 = float(today_data.get('SMA25', np.nan))
            t_open = float(today_data.get('Open', np.nan))
            t_high = float(today_data.get('High', np.nan))
            t_low = float(today_data.get('Low', np.nan))
            
            print(f"    🔍 [購入審査] {today_str} : {ticker}")
            print(f"       前日終値:{prev_close}, 始値:{t_open}, 高値:{t_high}")

            if pd.isna(sma25) or pd.isna(t_open) or pd.isna(prev_close): 
                print("       => ❌ データ欠損のため見送り")
                continue

            buy_price = None
            if t_open > prev_close:
                buy_price = t_open
                print(f"       => ⭕ 条件①クリア！ 始値({buy_price}円)で購入決定！")
            elif t_open <= prev_close and t_high > prev_close:
                buy_price = prev_close
                print(f"       => ⭕ 条件②クリア！ 前日終値({buy_price}円)で購入決定！")
            else:
                print("       => ❌ 条件未達 (寄り付きも安く、日中も前日終値を超えなかった) のため見送り")
                
            if buy_price is not None:
                cost = buy_price * POSITION_LOT
                if cash >= cost:
                    cash -= cost
                    positions[ticker] = {'entry_price': buy_price, 'shares': POSITION_LOT, 'entry_date': today_str}
                    print(f"       💰 資金確保OK！ {ticker} を {POSITION_LOT}株 購入しました！(残金: {cash:,.0f}円)")
                    
                    sl_price = sma25 * STOP_LOSS_PCT
                    if t_low <= sl_price:
                        cash += sl_price * POSITION_LOT
                        profit = (sl_price - buy_price) * POSITION_LOT
                        trade_history.append({'ticker': ticker, 'entry_date': today_str, 'exit_date': today_str, 'entry_price': buy_price, 'exit_price': sl_price, 'profit': profit, 'reason': "損切り(即日)"})
                        del positions[ticker]
                        print(f"       😭 しかし、買ったその日に急落し、即日損切り({sl_price}円)されました...")
                else:
                    print(f"       💸 残念！資金不足のため購入できませんでした (必要:{cost}円, 残金:{cash}円)")
