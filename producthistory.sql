select top 20
  'LILGREEN' as SupplierID,
  convert(varchar,convert(date,OrderDate)) as OrderDate,
  year(OrderDate) as Year,
  month(OrderDate) as Month,
  0 as Hour,
  datepart(quarter, OrderDate) as Quarter,
  Account as AccountID,
  Client.Name,
  cl.Code as GroupCode,
  cl.Description as GroupDecription,
  SalesRep.Code as RepCode,
  SalesRep.Name as RepName,
  si.Code as ProductID,
  si.Itemgroup as CategoryCode,
  sgc.Description as CategoryDescription,
  fQuantity as Ordered,
  fQtyProcessed as Delivered,
  fQuantityLineTotExcl as LineTotal,
  AveUCst as Cost
from [InvNum](nolock)
inner join Client(nolock) on Client.DCLink = InvNum.AccountID
inner Join CliClass(nolock) cl on Client.iClassID = cl.idCliClass
inner join Areas(nolock) a on client.iAreasID = a.idAreas
inner join SalesRep(nolock) on SalesRep.idsalesrep = client.repid
INNER JOIN _btblInvoiceLines(nolock) it on AutoIndex = iInvoiceID
inner join stkitem(nolock) si on it.istockcodeid = stocklink
inner join _bvStockGroups(nolock) sgc on sgc.stgroup = si.Itemgroup
where CONVERT(CHAR(4), orderdate, 12) =  '<nextmonth>'
order by orderdate
