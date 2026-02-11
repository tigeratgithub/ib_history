import asyncio
from ib_async import IB, ContFuture, Future, util

async def main():
    ib = IB()
    # ... (connection logic remains same, assuming port 4002 is correct)
    ports = [4002, 4001, 7497]
    connected = False
    for port in ports:
        try:
            await ib.connectAsync('127.0.0.1', port, clientId=99)
            connected = True
            break
        except: pass
    
    if not connected: return

    try:
        print("1. 尝试搜索 MNQ 期货合约 (Future) 以验证 Symbol...")
        # 搜索 MNQ 的期货合约，不带具体日期，看看返回什么
        mnq_fut = Future('MNQ', includeExpired=False)
        details = await ib.reqContractDetailsAsync(mnq_fut)
        
        if details:
            print(f"找到 {len(details)} 个 MNQ 合约。")
            c = details[0].contract
            print(f"第一个合约: Symbol={c.symbol}, LocalSymbol={c.localSymbol}, Exchange={c.exchange}, ConId={c.conId}")
            
            # 使用找到的第一个真实合约来测试数据权限
            print(f"\n2. 尝试获取该合约 ({c.localSymbol}) 的少量历史数据...")
            bars = await ib.reqHistoricalDataAsync(
                c,
                endDateTime='',
                durationStr='1 D',
                barSizeSetting='1 hour',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            print(f"成功获取 {len(bars)} 条数据 (单合约测试)。")
            if bars:
                print("最后5条数据：")
                for bar in bars[-5:]:
                    print(bar)

            # 再次尝试 ContFuture，这次使用从真实合约中获取的准确 Exchange 和 Symbol
            print(f"\n3. 再次尝试使用准确信息构建 ContFuture...")
            cont_contract = ContFuture(c.symbol, c.exchange)
            print(f"ContFuture 定义: {cont_contract}")
            
            bars_cont = await ib.reqHistoricalDataAsync(
                cont_contract,
                endDateTime='',
                durationStr='1 W', # 先试短一点
                barSizeSetting='1 hour',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            if bars_cont:
                print(f"成功获取 {len(bars_cont)} 条连续合约数据!")
                print("连续合约最后5条数据：")
                for bar in bars_cont[-5:]:
                    print(bar)
            else:
                print("连续合约获取失败。")
                
        else:
            print("未找到任何 MNQ 合约。可能是 Symbol 错误或没有数据订阅。")
            # 尝试搜索 MGC
            print("尝试搜索 MGC (微型黄金)...")
            mgc_fut = Future('MGC', includeExpired=False)
            details_mgc = await ib.reqContractDetailsAsync(mgc_fut)
            if details_mgc:
                 print(f"找到 MGC 合约: {details_mgc[0].contract.localSymbol}")
            else:
                 print("未找到 MGC 合约。")

    except Exception as e:
        print(f"发生异常: {e}")
    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
