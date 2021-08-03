
create multiset volatile table merch as (
select 
cbt.cus_cbt_merchant_id as merchant_id,
cbt.CUS_CBT_MERCHANT_NAME merchant_name,
cbt.sit_site_id as site_id,
cbt.ite_cbt_origin as origen,
cbt.cus_cbt_holding_group as holding_group,
cbt.first_bid_date_holding as first_bid_holding,
cbt.cus_cust_id as seller_id,
cus.cus_nickname as nickname,
case
when cbt.cus_cbt_flag_t1=1 then 'TRACK 1'
when cbt.cus_cbt_flag_t2=1 then 'TRACK 2'
when cbt.cus_cbt_flag_t3=1 then 'TRACK 3'
when cbt.cus_cbt_flag_t4=1 then 'TRACK 4'
end as TRACK,
rep.REP_CURRENT_LEVEL reputation,
ca.ASESOR


from  WHOWNER.lk_cus_cbt_item_origin cbt
left join WHOWNER.LK_CUS_CUSTOMERS_DATA as cus 
  on cbt.cus_cust_id = cus.cus_cust_id
left join BT_REP_SELLER_REPUTATION rep
      on cbt.cus_cust_id = rep.CUS_CUST_ID_SEL and rep.sit_site_id=cbt.sit_site_id
left join WHOWNER.LK_SALES_CARTERA_GESTIONADA_AC ca
   on cbt.cus_cust_id = ca.CUS_CUST_ID_SEL and ca.sit_site_id=cbt.sit_site_id and CA.ITE_OFFICIAL_STORE_ID = 0


)with data on commit preserve rows;


CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','CBT_CROSSLAUNCH_LL');
CREATE TABLE MELI_STAGING.CBT_CROSSLAUNCH_LL as (
select	

      a11.TIM_DAY,
      cbt.merchant_id,
      cbt.merchant_name,
      cbt.site_id,
      cbt.origen,
      cbt.holding_group,
      cbt.first_bid_holding,
      cbt.seller_id,
      cbt.nickname,
      cbt.TRACK,
      cbt.reputation,
      cbt.asesor,
	sum(a11.LIVELISTINGS)  LIVELISTINGSESELLER
	
from	WHOWNER.BT_LIVE_LISTINGS_SEL	a11

	inner join merch cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.seller_id

left outer join	WHOWNER.LK_MKT_MARKETPLACE	a12
	  on 	(a11.CAT_CATEG_ID_L1 = a12.CAT_CATEG_ID)
	  
where	a11.TIM_DAY  between DATE -3 and DATE -1
       and a12.MKT_MARKETPLACE_ID = 'TM'
       and a11.photo_id = 'TODATE'
       
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
with data primary index (TIM_DAY,SITE_ID,SELLER_ID);

CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','CBT_CROSSLAUNCH_SELLER');
CREATE TABLE MELI_STAGING.CBT_CROSSLAUNCH_SELLER as (
select 
cbt.merchant_id,
cbt.merchant_name,
cbt.site_id,
cbt.origen,
cbt.holding_group,
cbt.first_bid_holding,
cbt.seller_id,
cbt.nickname,
cbt.TRACK,
cbt.reputation,
cbt.asesor,

bid.tim_day_winning_date as tim_day,


sum( bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK)  GMVE,
sum(bid.BID_QUANTITY_OK)   SIE,

sum(ll.LIVELISTINGSESELLER) LIVELISTINGSESELLER


from  merch cbt

inner join WHOWNER.BT_BIDS bid
on cbt.seller_id=bid.cus_cust_id_sel
and cbt.site_id=bid.sit_site_id


LEFT JOIN MELI_STAGING.CBT_CROSSLAUNCH_LL LL 
  ON bid.cus_cust_id_sel=ll.seller_id
  and bid.tim_day_winning_date=ll.tim_day
  and bid.sit_site_id=ll.site_id

where bid.PHOTO_ID = 'TODATE'
and bid.ITE_GMV_FLAG = 1
and bid.tgmv_flag = 1 
and bid.MKT_MARKETPLACE_ID = 'TM'
and bid.tim_day_winning_date between DATE -120 and DATE -1


GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)with DATA primary index(MERCHANT_ID,seller_id,tim_day);


--Borra los dias ya insertados de la tabla final
DELETE FROM TABLEAU_TBL.CBT_CROSSLAUNCH_SELLER WHERE tim_day IN 
(SELECT DISTINCT 
      (tim_day) 
FROM MELI_STAGING.CBT_CROSSLAUNCH_SELLER);


--Inserta a la tabla productiva los dias nuevos
INSERT INTO TABLEAU_TBL.CBT_CROSSLAUNCH_SELLER
(merchant_id,
merchant_name,
site_id,
origin,
holding_group,
first_bid_holding,
seller_id,
nickname,
track,
reputation,
asesor,
tim_day,
last_refresh,
GMVE,
SIE,
LIVELISTINGSESELLER)
select 
    stg.merchant_id,
    stg.merchant_name,
    stg.site_id,
    stg.origen as origin,
    stg.holding_group,
    stg.first_bid_holding,
    stg.seller_id,
    stg.nickname,
    stg.track,
    stg.reputation,
    stg.asesor,
    stg.tim_day,
    DATE as last_refresh,
    stg.GMVE,
    stg.SIE,
    stg.LIVELISTINGSESELLER
from MELI_STAGING.CBT_CROSSLAUNCH_SELLER as stg;



