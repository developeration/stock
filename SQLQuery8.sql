declare @currentdate nvarchar(50) = '20230731'
declare @perdate7 varchar(50)=CONVERT(varchar(50), DATEADD(day,-7, CONVERT(datetime, @currentdate, 112) ),112)
declare @perdate14 varchar(50)=CONVERT(varchar(50), DATEADD(day,-14, CONVERT(datetime, @currentdate, 112) ),112)
declare @perdate28 varchar(50)=CONVERT(varchar(50), DATEADD(day,-28, CONVERT(datetime, @currentdate, 112) ),112)
declare @perdate56 varchar(50)=CONVERT(varchar(50), DATEADD(day,-56, CONVERT(datetime, @currentdate, 112) ),112)
declare @amountincreate int =2.5

declare @limitpricestart varchar(50)=CONVERT(varchar(50), DATEADD(year,-1.5, CONVERT(datetime, @currentdate, 112) ),112)
declare @pricelimitstep int =20

select 
stock_basic.ts_code,
dayBasicInfo.pe 
from [dbo].[dayBasicInfo](@currentdate) as dayBasicInfo
inner join [dbo].[stock_basic] as stock_basic
on dayBasicInfo.ts_code = stock_basic.ts_code
inner join [dbo].[summaryPrice](@perdate7,@currentdate) as summaryPriceA
on summaryPriceA.ts_code = stock_basic.ts_code
inner join [dbo].[summaryPrice](@perdate56,@perdate7) as summaryPriceB
on summaryPriceB.ts_code = stock_basic.ts_code
inner join [dbo].[priceScope](@limitpricestart,@pricelimitstep) as priceScope
on priceScope.ts_code = stock_basic.ts_code
where 
dayBasicInfo.pe>0--市盈率正
and dayBasicInfo.turnover_rate>1
and stock_basic.[list_date] <'20150000' --开户日期10年以前
--交易量放大，- 两周 --股价下跌中 
and summaryPriceA.amount > summaryPriceB.amount*@amountincreate
--and summaryPrice7.[close]<summaryPrice14.[close]
--历史低位
and summaryPriceA.[close]<priceScope.limitprice
and summaryPriceA.[close]>priceScope.minprice 
--换手率>1
and summaryPriceA.[close]>priceScope.minprice 
