/****** Script for SelectTopNRows command from SSMS  ******/
declare  @create_from varchar(50)= '20100000'
declare  @tradedate_from varchar(50)= '20210000'
declare  @step int= 42

--价格 1/42
;with pricelimit as (
SELECT ts_code,min([close]) as minprice,(max([close])-min([close]))/@step+min([close]) as limitprice
  FROM [Stock].[dbo].[daily]
  where trade_date > @tradedate_from
  group by ts_code 
  )
--市盈率正
, pelimit as (
select A.ts_code,A.pe from [Stock].[dbo].[daily_basic] as A,
(
SELECT ts_code,max(trade_date) as trade_date
  FROM [Stock].[dbo].[daily_basic]
  group by ts_code
  ) as B
where
A.ts_code=B.ts_code and  A.trade_date=B.trade_date 
and A.pe>0
) 
,
--近一周均价
weekavgprice as (
	select w.ts_code,AVG(w.[close]) as weeklyprice from 
	(
	 select A.* from [Stock].[dbo].[daily] as A,
	 (
	 SELECT ts_code,
	 CONVERT(varchar(50), DATEADD(day,-7, CONVERT(datetime, max(trade_date), 112) ),112)
	 as trade_date
	  FROM [Stock].[dbo].[daily]
	  group by ts_code
	  ) as B
	  where A.ts_code = B.ts_code 
	  and A.trade_date >= B.trade_date
	) as w
	group by w.ts_code
) 
,
--换手率
 weeklyturnover as (
	select w.ts_code,AVG(w.[turnover_rate]) as turnover from 
	(
	 select A.* from [Stock].[dbo].[daily_basic] as A,
	 (
	 SELECT ts_code,
	 CONVERT(varchar(50), DATEADD(day,-7, CONVERT(datetime, max(trade_date), 112) ),112)
	 as trade_date
	  FROM [Stock].[dbo].[daily_basic]
	  group by ts_code
	  ) as B
	  where A.ts_code = B.ts_code 
	  and A.trade_date >= B.trade_date
	) as w
	group by w.ts_code
)
 
select '"'+stock_basic.ts_code+'",' from 
pricelimit,pelimit,weekavgprice,
weeklyturnover,
--mturnoverx,
stock_basic
where
stock_basic.ts_code=pricelimit.ts_code
and
weeklyturnover.ts_code = pricelimit.ts_code
and
pricelimit.ts_code = pelimit.ts_code
and
pricelimit.ts_code = weekavgprice.ts_code
and
weekavgprice.weeklyprice>pricelimit.minprice
and
weekavgprice.weeklyprice < pricelimit.limitprice
and
stock_basic.[list_date]<@create_from
