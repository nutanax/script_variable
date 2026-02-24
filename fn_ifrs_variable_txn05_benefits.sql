CREATE OR REPLACE FUNCTION dds.fn_ifrs_variable_txn05_benefits(p_xtr_start_dt date)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
-- =============================================  
-- Author:  Narumol W.
-- Create date: 2021-05-25
-- Description: For insert event data 
-- 20Jun2022 - Narumol W. Change condition to get interest
-- 15Aug2022 - Narumol W. Add new datasource : dds.ifrs_premium_interest_chg
-- select dds.fn_ifrs_variable_txn05_benefits('2024-09-08'::date);
-- select * from dds.tb_fn_log order by log_dttm desc 
-- Oil 2022Sep27 : Add NAC Paylink 
-- Oil 01Nov2022 : ย้าย V3 ไว้หลัง dummy and chenge condition benefits_ytd as of V2 Dec2021 vs V2 current 
-- Oil 14Nov2022 : เพิ่ม policy_no , plan_cd
-- Oil 29Nov2022 : เพิ่ม บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน  , FAX CLAIM 
-- Oil 01Dec2022 : Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd (STEP10)
-- Oil 07Dec2022 : Log.236 แยก Reject Payment Trans  
-- Oil 07Dec2022 : Log.238 แยก Payment Trans
-- Oil 15Dec2022 : Log.237 Add New data source V13,V14 : dds.ifrs_payment_transpos_chg
-- Oil 15Dec2022 : Log.240 Add New data source V1:dds.ifrs_refund_trans_chg 
-- Oil 25jan2023 : policy_type not in ( 'M','G')
-- Oil 25jan2023 : add plan_cd 
-- Oil 07Feb2023 : Add V9 บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน 
-- 16Dec2022 Narumol W. : add key accode 
-- 14Feb2023 Narumol W.: V9 : Get data snapshot cheque_dec2023
-- Oil 14Feb2023 : Exclude from dummy system_type_cd = 015, 014
-- Oil 21Feb2023 : Track_chg.81 
-- Oil 08Mar2023 : Add TB, TLP expire cheque V9
-- 25Apr2023 Narumol W.: Update variable_cd to dummy_variable_nm
-- 12May2023 Narumol W.: ITRQ-66052132 : V9 ส่งข้อมูลแบบกลับ Sign 
-- Oil 20230609 :ITRQ#66052475 ขอปรับเงื่อนไข V2 BENEFIT_YTD สำหรับข้อมูลจาก system type 14,15 
-- 26Jun2023 Narumol W. Filter accode & trim plan cd
-- 03Jul2023 Narumol W. : [ITRQ#66062868] เปลี่ยนแปลง sign ของ CLAIM_YTD Var.6 และ BENEFIT Var.3
-- 09Aug2023 Narumol W. : [ITRQ#66083728] ขอเพิ่ม source payment_reject ใน V12 V13 V14 ของ Benefit_YTD
-- 09Aug2023 Narumol W. : [ITRQ#66083736] ขอเพิ่มเงื่อนไขการ mapping source nrcptyymm ใน V14 ของ Benefit_YTD ไม่รวม mortgate , group eb
-- 10Aug2023 Narumol W. : Log.382 เนื่องจากไม่มีเงื่อนไขในการ include LTB ใน stga_ifrs_nac_txn_step05_benefits จึงทำให้ ตก DUMMY ทั้งหมด จำเป็นต้อง Exclude ตั้งแต่ใน table : missing 
-- 18Aug2023 Narumol W. : [ITRQ#66083852] ขอเพิ่มเงื่อนไขของ V3 - Benefit_YTD
-- 20Sep2023 Narumol W. : [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
-- 02Oct2023 Narumol W. : [ITRQ#66104528] Patch Policy Data in VARIABLE_YTD
-- 03Oct2023 Narumol W. : [ITRQ#66104533] ขอเพิ่มเงื่อนไขของ V3 - Benefit_YTD : Exclude distribution_mode from key field 
-- 16Oct2023 Narumol W. : Use valid_val_fr_dttm in PVF condition
-- 03Nov2023 Narumol W. : [ITRQ#66104785] ขอปรับปรุงเงื่อนไข Coupon Dividend BENEFIT_YTD
-- 13Nov2023 Narumol W. : Benefit Log 403
-- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD
-- 22Dec2023 Narumol W. : Benefit Log. 412
-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
-- 23Feb2024 Narumol W. : [ITRQ#67020753] เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link
-- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition
-- 05Jun2024 Narumol W. : [ITRQ#67052463]เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link (enhance)
-- 10Jun2024 Narumol W. : [ITRQ#67073352] Enhance Variable Benefit - Coupon Dividend Condition
-- 11Sep2024 Narumol W. : [ITRQ#67094333] ขอเพิ่ม source paymant_transpos ใน V4 BENEFIT_YTD
-- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
-- 14Jul2024 Nuttadet O. : [ITRQ#68062201] เพิ่ม call stored dds.fn_ifrs_variable_txn05_imo_benefits
-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
-- =============================================  
declare 
	i int default  0;
	v_control_start_dt date;
	v_control_end_dt date;
	v_control_eoy_dt date;

	out_err_cd int default 0;
	out_err_msg varchar(200);

	v_affected_rows int;
	v_diff_sec int;
	v_current_time timestamp;

	v_variable_nm varchar(100);

BEGIN 
 

	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.fn_ifrs_variable_txn05_benefits','START: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

	if p_xtr_start_dt is not null then 
		v_control_start_dt := date_trunc('month', p_xtr_start_dt - interval '1 month');
		v_control_end_dt := v_control_start_dt + interval '1 month -1 day' ;
	end if;
	raise notice 'BENEFITS START ----------------------- % ',clock_timestamp() ;
	raise notice 'v_control_start_dt % ',v_control_start_dt::varchar(20) ;
	raise notice 'v_control_end_dt % ',v_control_end_dt::varchar(20) ;
 
	-- Prepare missing plancode select * from stag_s.stg_tb_core_planspec
	insert into stag_s.stg_tb_core_planspec ( plan_cd , ifrs17_var_current , index_flg )
	select plan_cd , ifrs_var_current,plan_index
	from (   
	 select 'GEB1' as plan_cd ,'Y' as ifrs_var_current ,null as ifrs17_var_future,'TERM' as plan_index union all 
	 select 'GEB2' as plan_cd ,'Y' as ifrs_var_current ,null as ifrs17_var_future,'TERM' as plan_index  union all 
	 select 'PL34' as plan_cd ,'Y' as ifrs_var_current ,'Y' as ifrs17_var_future ,'TERM' as plan_index   union all 
	 select 'M907' as plan_cd ,'Y' as ifrs_var_current ,'Y' as ifrs17_var_future ,'TERM' as plan_index  
	 ) pl   
	 where plan_cd not in ( select plan_cd from stag_s.stg_tb_core_planspec  );

	 -- Prepare accrual 
	select dds.fn_dynamic_vw_ifrs_accrual_chg (v_control_start_dt , v_control_end_dt) into out_err_cd;
	select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg(v_control_start_dt) into out_err_cd;
	-- Prepare Data APL from Actuary
	select dds.fn_dynamic_vw_ifrs_apl (v_control_start_dt , v_control_end_dt) into out_err_cd;
	select  dds.fn_ifrs_variable_txn01_04_eoy(p_xtr_start_dt) into out_err_cd;
	
  
	-- 00. Prepare Summary table 
	-- 00.1 Coupon paid
	BEGIN 
		/*
		--drop table if exists stag_a.stga_ifrs_coupon_paid_summary; 	
		--create table stag_a.stga_ifrs_coupon_paid_summary tablespace tbs_stag_a as 		
		truncate table stag_a.stga_ifrs_coupon_paid_summary;
		insert into stag_a.stga_ifrs_coupon_paid_summary
		select  policy_no,pay_dt
		,max(is_interest) as is_interest
		,max(is_interest) as is_deposit
		,sum(paycmast_amt)::numeric(20,2) as paycmast_amt
		,sum(interest_amt)::numeric(20,2)  as interest_amt
		from ( 
		select policy_no,pay_dt
		,paycmast_amt::numeric(20,2) as paycmast_amt,interest_amt::numeric(20,2) as interest_amt
		,case when interest_amt > 0 then 1 else 0 end as is_interest 
		from  stag_a.stga_ifrs_coupon_paid
		where pay_flg <> '2' 
		and pay_dt  between v_control_start_dt and v_control_end_dt 
		) as sum_paycmast
		group by policy_no,pay_dt ; 
	 
		drop table if exists stag_a.stga_ifrs_coupon_paid_summary; 	
		create table stag_a.stga_ifrs_coupon_paid_summary tablespace tbs_stag_a as
		*/
		truncate table stag_a.stga_ifrs_coupon_paid_summary;
		insert into stag_a.stga_ifrs_coupon_paid_summary
		select policy_no ,pay_dt ,1::int4 as is_interest ,1::int4 as is_deposit
		, sum(posting_amount) as  interest_amt
		from dds.oic_nac_chg onc 
		where doc_dt  between v_control_start_dt and v_control_end_dt 
		and accode in ( '5031020020' )  
		group by policy_no ,pay_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		--CREATE INDEX stga_ifrs_coupon_paid_summary_policy_no_idx ON stag_a.stga_ifrs_coupon_paid_summary (policy_no,is_deposit);
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP00 Prepare stga_ifrs_paycmast_summary : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP01 % : % row(s) - Prepare Summary table Coupon paid ',clock_timestamp()::varchar(19),v_affected_rows::varchar;


	-- 00.2 Dividend paid
	begin
		/*
		--drop  table if exists stag_a.stga_ifrs_dividend_paid_summary; 	
		--create table stag_a.stga_ifrs_dividend_paid_summary tablespace tbs_stag_a as 
		truncate table stag_a.stga_ifrs_dividend_paid_summary;
		insert into stag_a.stga_ifrs_dividend_paid_summary
		select  policy_no,pay_dt
		,max(is_interest) as is_interest
		,max(is_interest) as is_deposit
		,sum(dvmast_amt) as dvmast_amt
		,sum(interest_amt) as interest_amt
		from ( 
		select policy_no,pay_dt,dvmast_amt,interest_amt 
		,case when interest_amt > 0 then 1 else 0 end as is_interest 
		from  stag_a.stga_ifrs_dividend_paid 
		where pay_flg <> '2' 
		and pay_dt  between v_control_start_dt and v_control_end_dt 
		) as sum_paycmast
		group by policy_no,pay_dt;  
		drop  table if exists stag_a.stga_ifrs_dividend_paid_summary; 	
		create table stag_a.stga_ifrs_dividend_paid_summary tablespace tbs_stag_a as 	 
		*/
		truncate table stag_a.stga_ifrs_dividend_paid_summary;
		insert into stag_a.stga_ifrs_dividend_paid_summary
		select policy_no ,pay_dt ,1::int4 as is_interest ,1::int4 as is_deposit
		, sum(posting_amount) as  interest_amt
		from dds.oic_nac_chg onc 
		where doc_dt  between v_control_start_dt and v_control_end_dt 
		and accode in ( '5031020030' ) 	
		group by policy_no ,pay_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		--CREATE INDEX stga_ifrs_dividend_paid_summary_policy_no_idx ON stag_a.stga_ifrs_dividend_paid_summary (policy_no,is_deposit);
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP00 Prepare stga_ifrs_dvmast_summary : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP02 % : % row(s) - Prepare Summary table Dividend paid ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 00.3 Coupon paid
	BEGIN
		--drop  table if exists stag_a.stga_ifrs_coupon_unpaid_summary; 	
		--create table stag_a.stga_ifrs_coupon_unpaid_summary tablespace tbs_stag_a as 
		truncate table stag_a.stga_ifrs_coupon_unpaid_summary;
		insert into stag_a.stga_ifrs_coupon_unpaid_summary
		select  policy_no
		,max(is_interest) as is_interest
		,max(is_interest) as is_deposit
		,sum(pcunpaid_amt) as pcunpaid_amt
		,sum(interest_amt) as interest_amt
		from ( 
		select policy_no,pcunpaid_amt,interest_amt 
		,case when interest_amt > 0 then 1 else 0 end as is_interest 
		from   dds.ifrs_coupon_unpaid_append
		where source_file_dt between v_control_start_dt and v_control_end_dt 
		) as sum_paycmast
		group by policy_no ;  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		--CREATE INDEX stga_ifrs_coupon_unpaid_summary_policy_no_idx ON stag_a.stga_ifrs_coupon_unpaid_summary (policy_no,is_interest);
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP00 Prepare stga_ifrs_coupon_unpaid_summary : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;  
	raise notice 'STEP03 % : % row(s) - Prepare Summary Ftable Coupon unpaid ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- 00.4 Dividend unpaid -- select * from  stag_a.stga_ifrs_dividend_unpaid_summary; 
	BEGIN
		--drop  table if exists stag_a.stga_ifrs_dividend_unpaid_summary; 	
		--create table stag_a.stga_ifrs_dividend_unpaid_summary tablespace tbs_stag_a as 
		truncate table stag_a.stga_ifrs_dividend_unpaid_summary;
		insert into stag_a.stga_ifrs_dividend_unpaid_summary
		select  policy_no 
		,max(is_interest) as is_interest
		,max(is_interest) as is_deposit
		,sum(dvunpaid_amt) as dvunpaid_amt
		,sum(interest_amt) as interest_amt
		from ( 
		select policy_no,dvunpaid_amt,interest_amt  
		,case when interest_amt > 0 then 1 else 0 end as is_interest 
		from dds.ifrs_dividend_unpaid_append
		where source_file_dt between v_control_start_dt and v_control_end_dt 
		) as sum_paycmast
		group by policy_no;  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		--CREATE INDEX stga_ifrs_dividend_unpaid_summary_policy_no_idx ON stag_a.stga_ifrs_dividend_unpaid_summary (policy_no,is_interest);
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP00 Prepare stga_ifrs_dvmast_summary : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP04 % : % row(s) - Prepare Summary table Dividend unpaid ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

  

 	-- ===================================  V4 จ่าย Surrender เป็นเงินสด =========================== -- 
	BEGIN
		--drop table if exists stag_a.stga_ifrs_nac_txn_step05_benefits;
		--create table stag_a.stga_ifrs_nac_txn_step05_benefits tablespace tbs_stag_a as 
		truncate table stag_a.stga_ifrs_nac_txn_step05_benefits;

 		-- 14Jul2024 Nuttadet O. : [ITRQ#68062201] เพิ่ม call stored dds.fn_ifrs_variable_txn05_imo_benefits
		select dds.fn_ifrs_variable_txn05_imo_benefits(p_xtr_start_dt) into out_err_cd  ; 

		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar(20) as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Surrender'::varchar(100) as subgroup_nm , 'จ่าย Surrender เป็นเงินสด' ::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V4'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		 left outer join dds.tl_acc_policy_chg pol    
		on ( nac.policy_no = pol.policy_no
		and nac.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		where  natrn.post_dt  between v_control_start_dt and v_control_end_dt;
		-- and pp.index_flg = 'UL' -- Log No. 64 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP05 V4 Surrender : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	--CREATE INDEX  stga_ifrs_nac_txn_step05_benefits_nac_rk_idx ON stag_a.stga_ifrs_nac_txn_step05_benefits USING btree ( nac_rk);
	--CREATE INDEX  stga_ifrs_nac_txn_step05_benefits_source_filename_idx ON stag_a.stga_ifrs_nac_txn_step05_benefits USING btree ( source_filename);
	 
	raise notice 'STEP05 % : % row(s) - V4 จ่าย Surrender เป็นเงินสด',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	  

	-- 11Sep2024 Narumol W. : [ITRQ#67094333] ขอเพิ่ม source paymant_transpos ใน V4 BENEFIT_YTD
	BEGIN  
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm   , 'Surrender'::varchar(100) as subgroup_nm , 'จ่าย Surrender เป็นเงินสด' ::varchar(100) as subgroup_desc   
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg paytrans  
		on ( natrn.org_branch_cd =  paytrans.ref_branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt )
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd    
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V4'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		--where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.payment_transpos_rk  ) 
	    where natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP05 V4 Surrender : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;   
	raise notice 'STEP05 % : % row(s) - V4 จ่าย Surrender เป็นเงินสด',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 11Sep2024 Narumol W. : [ITRQ#67094333] ขอเพิ่ม source ifrs_payment_reject_chg ใน V4 BENEFIT_YTD

	begin  
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  , 'Reject Surrender' as subgroup_nm ,'Reject จ่าย Surrender เป็นเงินสด'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit' 
		 and  vn.variable_cd = 'V4'
		 and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP19 V7 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP05 % : % row(s) - V4 Reject จ่ายดอกเบี้ยของผลประโยชน์',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 
	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
	begin  
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt,reject.doc_type,reject.doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel , null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse Surrender' as subgroup_nm ,'PAYMENT API Reverse จ่าย Surrender เป็นเงินสด'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail , reject.branch_cd as org_branch_cd 
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
        inner join  dds.ifrs_payment_api_reverse_chg reject  
        on ( natrn.branch_cd  =  reject.branch_cd 
        and natrn.doc_dt = reject.doc_dt 
        and natrn.doc_type =  reject.doc_type 
        and natrn.doc_no = reject.doc_no 
        and natrn.dc_flg = reject.dc_flg
        and natrn.accode = reject.accode 
        and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 		
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit' 
		 and  vn.variable_cd = 'V4'
		 and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP19 V4 api_reverse : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP05 % : % row(s) - V4 api_reverse จ่ายดอกเบี้ยของผลประโยชน์',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 

	-- ===================================  V1 บันทึกจ่ายเงินผลประโยชน์ Dividend แบบไม่เคยฝากมาก่อน ======================================================== --
 	BEGIN
		--drop table if exists stag_a.stga_ifrs_nac_txn_step05_benefits;
		--create table stag_a.stga_ifrs_nac_txn_step05_benefits tablespace tbs_stag_a as 
		--truncate table stag_a.stga_ifrs_nac_txn_step05_benefits ; 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-dividend_paid'::varchar(100) as  group_nm , 'Pay Benefits Dividend'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์ Dividend แบบไม่เคยฝากมาก่อน' ::varchar(100) as subgroup_desc  
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 		
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
 		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V1'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg
		 )		
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd  
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm  )
		 where nac.filename <> 'nacpaylink'
		 and natrn.benefit_gmm_flg  = '2.2' -- 2.2	Dividend - INV
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk ) 
		 and not exists ( select 1 from stag_a.stga_ifrs_dividend_paid_summary dv 
		 					where dv.policy_no = nac.policy_no 
		 					and dv.is_deposit = 1 ) 
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP06 V1 Pay Benefits Dividend แบบไม่เคยฝากมาก่อน : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP06 % : % row(s) - V1 Pay Benefits Dividend แบบไม่เคยฝากมาก่อน ',clock_timestamp()::varchar(19),v_affected_rows::varchar; 

	-- ===================================  V1 บันทึกจ่ายเงินผลประโยชน์ Coupon แบบไม่เคยฝากมาก่อน ======================================================== --
 	BEGIN
		--drop table if exists stag_a.stga_ifrs_nac_txn_step05_benefits;
		--create table stag_a.stga_ifrs_nac_txn_step05_benefits tablespace tbs_stag_a as 
		--truncate table stag_a.stga_ifrs_nac_txn_step05_benefits ; 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,nac.posting_amount as posting_amt-- posting_sum_nac_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-coupon_paid'::varchar(100) as  group_nm , 'Pay Benefits Coupon'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์ Coupon แบบไม่เคยฝากมาก่อน' ::varchar(100) as subgroup_desc  
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 		
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
 		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V1'
		 and natrn.event_type = vn.event_type 
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg
		 )		 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where nac.filename <> 'nacpaylink'
		 and natrn.benefit_gmm_flg  = '8' -- 8	Coupon
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk ) 
		 and not exists ( select 1 from stag_a.stga_ifrs_coupon_paid_summary cp 
		 								where cp.policy_no = nac.policy_no and cp.is_deposit = 1 ) 
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt; 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP07 V1 Pay Benefits Coupon แบบไม่เคยฝากมาก่อน : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP07 % : % row(s) - V1 Pay Benefits Coupon แบบไม่เคยฝากมาก่อน',clock_timestamp()::varchar(19),v_affected_rows::varchar; 

/*
	-- ===================================  V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ ======================================================== --
	raise notice 'START STEP09 % : % row(s) - V1 Pay Benefits nacpaylink',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	BEGIN
		--drop table if exists stag_a.stga_ifrs_nac_txn_step05_benefits;
		--create table stag_a.stga_ifrs_nac_txn_step05_benefits tablespace tbs_stag_a as 
		--truncate table stag_a.stga_ifrs_nac_txn_step05_benefits ; 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,nac.posting_amount as posting_amt-- posting_sum_nac_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd 
		,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nacpaylink-coupon_paid'::varchar(100) as  group_nm , 'Pay Benefits nacpaylink'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04  natrn 
		inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt  
		and natrn.sales_id = nac.sales_id)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on (  nac.policy_no = pol.policy_no
		and nac.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V1'
		 and natrn.event_type = vn.event_type 
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )		
		 where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk )
		 and not exists ( select 1 from stag_a.stga_ifrs_coupon_paid_summary cp where cp.policy_no = nac.policy_no and cp.is_deposit = 1 )
		 --and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
		 and natrn.post_dt  between '2022-02-01' and '2022-02-28';

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP09 V1 Pay Benefits - nacpaylink : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP09 % : % row(s) - V1 Pay Benefits nacpaylink',clock_timestamp()::varchar(19),v_affected_rows::varchar;
*/
	-- ===================================  V1 account AC Paylink  บันทึกจ่ายเงินผลประโยชน์ ======================================================== --
 	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1,acpay.account_acpaylink_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, acpay.doc_dt,acpay.doc_type,acpay.doc_no
		, acpay.accode,acpay.dc_flg 
		, acpay.actype as actype 
		, acpay.acpaylink_amt 
		, acpay.acpaylink_posting_amt as posting_amt
		, acpay.system_type as system_type,acpay.trans_type_cd as transaction_type,''::varchar as premium_type 
		, acpay.policy_no ,acpay.plan_cd, acpay.rider_type_cd as rider_cd ,null::date as pay_dt ,acpay.method_type as pay_by_channel
		,null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt  
		, acpay.sales_id,acpay.sales_struct_n,acpay.selling_partner,acpay.distribution_mode,acpay.product_group,acpay.product_sub_group,acpay.rider_group,acpay.product_term,acpay.cost_center
		, acpay.acpaylink_dim_txt 
		, 'acpaylink' as source_filename , 'natrn-acpaylink' as  group_nm  , 'Surrender' as subgroup_nm , 'บันทึกการจ่ายเงินคืน'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		,natrn.detail as nadet_detail ,natrn.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,acpay.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg  ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from dds.ifrs_account_acpaylink_chg acpay  -- select * from dds.ifrs_account_acpaylink_chg 
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.doc_dt = acpay.doc_dt
		and natrn.doc_type = acpay.doc_type
		and natrn.doc_no = acpay.doc_no 
		and natrn.dc_flg = acpay.dc_flg
		and natrn.accode = acpay.accode
		and natrn.natrn_dim_txt = acpay.acpaylink_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( acpay.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( acpay.policy_no = pol.policy_no
		 and acpay.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V1'
		 and natrn.event_type = vn.event_type 
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )		
		 where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = acpay.account_acpaylink_rk  and bb.group_nm =  'natrn-acpaylink' )
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP08 V1 Pay Benefits - acpaylink : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP08 % : % row(s) - V1 Pay Benefits acpaylink',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
	 
	-- ===================================  V1 Refund Trans บัญชีเงินคืนตามเงื่อนไขกรมธรรม์กรณีไม่เคลม ======================================================== --

	--17Jan2024 Narumol W. : Benefit Log 425 : Work around until model fixed 

	truncate table stag_a.ifrs_refund_trans_chg_no_dup;
	insert into stag_a.ifrs_refund_trans_chg_no_dup
	select *
	from (  
	select refund_trans_rk,natrn_rk,refund_x_dim_rk,processing_branch_cd,org_pbranch_cd,branch_cd,org_branch_cd,system_type
	,refund_dt,refund_key,refund_type,seq_no,ac_type,accode,account_nm,dc_flg,doc_dt,doc_type,doc_no,refund_amt,posting_refund_amt
	,reference_no,transaction_type,premium_type,for_branch_cd,submit_no,rp_no,account_flg,user_id,transaction_dt
	,policy_type,policy_no,plan_cd,rider_cd,pay_period,sales_id,sales_struct_n
	,selling_partner,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center,refund_dim_txt
	,valid_from_dttm,valid_to_dttm
	,check_sum,ins_wf_run_id,ppn_tm,upd_wf_ins_id,upd_tm 
	, row_number() over  ( partition by refund_x_dim_rk,refund_x_dim_rk,branch_cd , doc_dt ,doc_type ,doc_no ,accode ,policy_no ,rider_cd ,check_sum
	order by refund_x_dim_rk,refund_x_dim_rk,branch_cd , doc_dt ,doc_type ,doc_no ,accode ,policy_no ,rider_cd ,check_sum,valid_from_dttm desc ) as _rk 
	from dds.ifrs_refund_trans_chg   
	) as a 
	where _rk = 1 ;

	begin
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select natrn.ref_1 as ref_1 ,paytrans.natrn_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
        , paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
        , paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
        , paytrans.refund_amt 
        , paytrans.posting_refund_amt   as posting_amt
        , paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
        , paytrans.policy_no as policy_no ,paytrans.plan_cd as plan_cd , paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
        ,null::varchar as pay_period
        , posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        , paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
        , paytrans.refund_dim_txt as refund_dim_txt  
        , 'refund_trans'::varchar as source_filename  , 'natrn-refund_trans' as  group_nm  , 'No claim bonus' as subgroup_nm ,'เงินคืนตามเงื่อนไขกรมธรรม์กรณีไม่เคลม'  as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm 
        , natrn.detail as nadet_detail 
        ,paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
        ,paytrans.for_branch_cd
        ,natrn.event_type 
        ,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
        from  stag_a.stga_ifrs_nac_txn_step04 natrn 
        inner join stag_a.ifrs_refund_trans_chg_no_dup paytrans --dds.ifrs_refund_trans_chg  paytrans  
        on ( natrn.branch_cd =  paytrans.branch_cd 
        and natrn.doc_dt = paytrans.doc_dt
        and natrn.doc_type =  paytrans.doc_type 
        and natrn.doc_no = paytrans.doc_no 
        and  natrn.dc_flg = paytrans.dc_flg
        and natrn.accode = paytrans.accode 
        and natrn.natrn_dim_txt = paytrans.refund_dim_txt  ) 
        left join stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  ) 
        left join  stag_s.stg_tb_core_planspec pp 
        on ( paytrans.plan_cd = pp.plan_cd) 
        left outer join dds.tl_acc_policy_chg pol  
        on ( paytrans.policy_no = pol.policy_no 
        and paytrans.plan_cd = pol.plan_cd 
        and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
        inner join stag_s.ifrs_common_variable_nm vn 
        on ( vn.event_type = 'Benefit'
        and vn.variable_cd = 'V1'  
        and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
        where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;

       GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP09 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP09 % : % row(s) - V1 Refund Trans',clock_timestamp()::varchar(19),v_affected_rows::varchar;
    


 	-- ===================================  V1 Accrued =========================== -- 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1, accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header , natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt nac_amt 
 		, accru.posting_accru_amount  posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		, null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm,'Surrender,Force Surrender,Others'::varchar(100) as subgroup_nm , 'Surrender,Force Surrender,Others'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V1'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)	
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt 
		 and natrn.benefit_gmm_flg in ( '5','10.1','12.2')
		 and accru.system_type_cd = '037' 
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = accru.accrual_rk and bb.source_filename = 'accrued-ac'); 
		 
		 GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP09 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP09 % : % row(s) - V1 Benefit - Surrender, Force surrender_Inv',clock_timestamp()::varchar(19),v_affected_rows::varchar;
    
 	/* ย้ายไปอยู่หลัง V7
  	-- ===================================  V1 Payment Trans =========================== -- 
	begin
		-- V1 - Coupon แบบไม่มีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and trim(paytrans.policy_type) = trim(pol.policy_type)   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
		and natrn.benefit_gmm_flg = '8' -- Log.236
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V1 Paytrans - Coupon แบบไม่มีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		-- V1 - Dividend แบบไม่มีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and trim(paytrans.policy_type) = trim(pol.policy_type)   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V1 Paytrans - Dividend แบบไม่มีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
		-- V1 - ไม่มีดอกเบี้ย Exclude Coupon,Dividend
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and trim(paytrans.policy_type) = trim(pol.policy_type)   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg not in ('8','2.1','2.2')
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V1 Paytrans - ไม่มีดอกเบี้ย Exclude Coupon,Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
		-- V5 - Coupon แบบมีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and trim(paytrans.policy_type) = trim(pol.policy_type)   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V5'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg in ('8')-- Log.236  Coupon
		and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V5 Paytrans - Coupon แบบมีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		-- V6 - Dividend แบบมีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and trim(paytrans.policy_type) = trim(pol.policy_type)   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V6'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V6 Paytrans - Dividend แบบมีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
	
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP10 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP10 % : % row(s) - Paytrans จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
  	-- ===================================  Reject Payment Trans =========================== -- 
	
 	begin -- Log.147 K.Ball CF to add data source
		--=== V1 - Coupon แบบไม่มีดอกเบี้ย  
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no = reject.policy_no )
		and natrn.benefit_gmm_flg = '8' -- Log.236
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) -  V1 Reject Coupon แบบไม่มีดอกเบี้ย ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	   --=== V1 - Dividend แบบไม่มีดอกเบี้ย
	    insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
		and natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) - V1 Reject Dividend แบบไม่มีดอกเบี้ย ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		--แบบไม่มีดอกเบี้ย Exclude 8-Coupon,2.1-Dividend - INS,2.2-Dividend - INV
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
		and natrn.benefit_gmm_flg not in ('8','2.1','2.2') 
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) - V1 ไม่มีดอกเบี้ย Exclude Coupon,Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	   
	
	   --=== V5 - Coupon แบบมีดอกเบี้ย  
	   --Oil 07Dec2022 : Log.236  V5 - Coupon แบบมีดอกเบี้ย  
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	   select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V5'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where   not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
	    and  natrn.benefit_gmm_flg = '8'
		and  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) -- จ่ายพร้อมดอกเบี้ย
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	    raise notice 'STEP11 % : % row(s) - Coupon แบบมีดอกเบี้ย  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	  
	    --=== V6 - Dividend แบบมีดอกเบี้ย   
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	   select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V6'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where   not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
	    and  natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		and  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) -- จ่ายพร้อมดอกเบี้ย
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	    raise notice 'STEP11 % : % row(s) - Dividend แบบมีดอกเบี้ย  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	    
	   
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP11 Reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP11 % : % row(s) - Reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	*/
 	---========================= V1 - FAX CLAIM Track_chg 48 =======================------
	begin 
		
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select aa.ref_1 , aa.adth_rk --, aa.adth_pay_rk 
						, aa.natrn_x_dim_rk
                        , aa.branch_cd , aa.sap_doc_type , aa.ref1_header
                        , aa.post_dt --, aa.claim_dt , aa.accident_dt , aa.claim_ok_dt
                        , aa.doc_dt , aa.doc_type , aa.doc_no , aa.accode , aa.dc_flg , aa.actype
                        , aa.claim_amt , aa.posting_claim_amt 
                        , aa.system_type, aa.transaction_type , aa.premium_type
                        , aa.policy_no , aa.plan_cd , aa.rider_cd   
                        , aa.pay_dt 
                        , aa.pay_by_channel
                        , null::varchar as pay_period
                        , aa.sum_natrn_amt , aa.posting_sap_amt , aa.posting_proxy_amt                      
                       -- , aa.posting_claim_pay_amt , aa.claim_pay_amt                         
                        , aa.sales_id , aa.sales_struct_n
                        , aa.selling_partner , aa.distribution_mode , aa.product_group , aa.product_sub_group
                        , aa.rider_group , aa.product_term , aa.cost_center
                        , aa.adth_dim_txt , aa.source_filename
                        , aa.group_nm , aa.subgroup_nm , aa.subgroup_desc
                        , vn.variable_cd
                        , vn.variable_name as variable_nm
                        , aa.nadet_detail , aa.org_branch_cd , aa.org_submit_no , aa.submit_no
                        , aa.section_order_no , aa.is_section_order_no
                        , aa.for_branch_cd , aa.event_type
                        , aa.benefit_gmm_flg , aa.benefit_gmm_desc
                        , aa.policy_type , aa.effective_dt , aa.issued_dt
                        , aa.ifrs17_channelcode , aa.ifrs17_partnercode , aa.ifrs17_portfoliocode
                        , aa.ifrs17_portid , aa.ifrs17_portgroup
                        , vn.is_duplicate_variable
                        , vn.duplicate_fr_variable_nm
                from (                          
               			 select  gl.ref1_header as ref_1 ,adth.adth_fax_claim_rk as adth_rk , null::bigint natrn_x_dim_rk  
                        ,adth.branch_cd 
                        ,gl.doc_type as sap_doc_type ,left(gl.ref1_header ,6) as ref1_header ,gl.posting_date_dt as post_dt                         
                        ,coalesce(adth.doc_dt,gl.doc_dt ) as doc_dt ,adth.doc_type,adth.doc_no,gl.account_no as accode,adth.dc_flg,null::varchar as actype 
                        ,adth.claim_amt,adth.posting_claim_amt                          
                        ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
                        ,adth.policy_no,adth.plan_cd, adth.rider_cd
                        ,null::date pay_dt  
                        ,null::varchar pay_by_channel
                        ,0::numeric as sum_natrn_amt  
                        ,gl.posting_sap_amt,gl.posting_sap_amt as posting_proxy_amt
                        ,adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
                        ,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
                        ,adth.adthtran_dim_txt as adth_dim_txt
                        ,adth.source_filename , 'sap-fax claim' as  group_nm  , 'sap-fax claim' as subgroup_nm ,  'Fax Claim' as subgroup_desc 
                         ,'V1'::varchar  as variable_cd   
                        , gl.description_txt  as nadet_detail 
                        , adth.org_branch_cd
                        , null::varchar as org_submit_no
                        , null::varchar as submit_no
                        , adth.section_order_no
                        , 1 as is_section_order_no
                        , adth.branch_cd as for_branch_cd
                        , coa.event_type  
                        , coa.benefit_gmm_flg,coa.benefit_gmm_desc 
                        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
                        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
                        ,pp.ifrs17_portid ,pp.ifrs17_portgroup  
                        from stag_a.stga_ifrs_nac_txn_step01 gl  
                        inner join  dds.ifrs_adth_payment_api_chg adth  
                        on ( gl.ref1_header = adth.ref_header 
                        and gl.account_no  = adth.accode 
                        and gl.sap_dim_txt = adth.adthtran_dim_txt)               
                        left outer join stag_s.ifrs_common_coa coa
                        on ( gl.account_no = coa.accode )
                        left join stag_s.ifrs_common_accode  acc 
                        on ( gl.account_no = acc.account_cd  )  
                         left join  stag_s.stg_tb_core_planspec pp 
                         on ( adth.plan_cd = pp.plan_cd) 
                         left outer join dds.tl_acc_policy_chg pol  
                         on (  adth.policy_no = pol.policy_no
                         and adth.plan_cd = pol.plan_cd
                         and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
                        where coa.event_type = 'Benefit'    
                         and gl.doc_type  = 'KI'
                         and org_branch_cd in ( 'CCL','GCL' )
                         and gl.posting_date_dt between v_control_start_dt and v_control_end_dt                        
                         ) as aa 
                         inner join stag_s.ifrs_common_variable_nm vn
                         on ( vn.event_type = 'Benefit'  
                         and vn.variable_cd = aa.variable_cd 
                         and vn.benefit_gmm_flg  = aa.benefit_gmm_flg 
                         ) ;
                              
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP11 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP11 % : % row(s) - V1 FAX CLAIM',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
 	
 
 
	/* -- Oil 01Nov2022 : ย้ายไปไว้หลัง dummy and chenge condition benefits_ytd as of V2 Dec2021 vs V2 current 	
 	-- ===================================  V3 รายการจ่ายของเงินผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน =========================== -- 
	BEGIN
		select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg(v_control_start_dt) into out_err_cd;

		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select  accru.ref_1 as ref_1,accru.accrual_rk as nac_rk ,  natrn.natrn_x_dim_rk  
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, v_control_end_dt as  post_dt  --natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt , accru.posting_accru_amount as posting_amt 
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		, null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accrued Claim' as subgroup_nm , 'รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,coa.event_type  
		,coa.benefit_gmm_flg,coa.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from dds.vw_ifrs_accrual_eoy_chg accru 
		left outer join stag_s.ifrs_common_coa coa
		on ( accru.accode = coa.accode )   
		left join stag_a.stga_ifrs_nac_txn_step04_eoy natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.accode = accru.accode
		and natrn.dc_flg = accru.dc_flg 
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( natrn.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V3'
		 and vn.benefit_gmm_flg  = coa.benefit_gmm_flg  
		 )	
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where   exists ( select 1 
		 					from dds.ifrs_variable_benefits_ytd ivby 
		 					where accru.policy_no = ivby.policy_no 
						 	and accru.accode = ivby.accode
						 	and accru.plan_code = ivby.plan_cd
						 	and accru.rider_code = ivby.rider_cd 
						 	and accru.transaction_type = ivby.transaction_type  
						 	and ivby.control_dt = and control_dt = date_trunc('day',(date_trunc('year', p_xtr_start_dt::timestamp -interval '1 second')-interval '1 second'))
						    and ivby.variable_cd ='V2' -- Oil 01Nov2022 condition : V2 Dec ปีที่แล้ว มาเช็คว่า ไม่มีอยู่ใน V2 เดือนที่รัน ( Current )
		 and not exists ( select 1 
		 					from dds.vw_ifrs_accrual_chg  cur  
		 					where accru.policy_no = cur.policy_no 
						 	and accru.accode = cur.accode
						 	and accru.plan_code = cur.plan_code 
						 	and accru.rider_code = cur.rider_code 
						 	and accru.ref_no = cur.ref_no 
						 	and accru.transaction_type = cur.transaction_type  
						 	)  
		 and coalesce(natrn.is_accru,0) <> 1  
		 --and accru.system_type_cd <> '037'; -- Log 224 
  		 --and  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP12 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP12 % : % row(s) - V3 รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน',clock_timestamp()::varchar(19),v_affected_rows::varchar;
    */
 
 	---=================================== V1 ACCRUAL_N1 ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link ==============================
 	-- 23Feb2024 Narumol W.: [ITRQ#67020753] เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link
	BEGIN
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
	 		select  accru.ref_1 as ref_1,0::int as accrual_n1_rk, null::int as natrn_x_dim_rk 
	 		,accru.branch_cd ,null::varchar sap_doc_type,accru.ref_1 reference_header , accru.doc_dt as post_dt 
			, accru.doc_dt,accru.doc_type,accru.doc_no
			, accru.accode,accru.dc_flg 
			, accru.ac_type 
			, accru.accru_amt nac_amt 
	 		, accru.posting_accru_amount  posting_amt
			, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
			, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
			, null::date as pay_dt ,accru.pay_by as pay_by_channel
			, accru.pay_period as pay_period
			, 0::int as sum_natrn_amt ,0::int posting_sap_amt,0::int posting_proxy_amt   
			, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group
			, accru.product_term,accru.cost_center 
			, accru.accru_dim_txt
			, 'dds.ifrs_accrual_n1_chg' as source_filename , 'accrual_n1' as  group_nm
			,'ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link'::varchar(100) as subgroup_nm , 'ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link'  as subgroup_desc 
			, vn.variable_cd ,vn.variable_name  
			, ''::varchar as nadet_detail ,accru.org_branch_cd
			, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
			,accru.for_branch as for_branch_cd
			,coa.event_type 
			,coa.benefit_gmm_flg,coa.benefit_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup 
			,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
			from dds.ifrs_accrual_n1_chg accru   
			left join  stag_s.ifrs_common_coa coa 
			on ( coa.event_type = 'Benefit'
			and accru.accode = coa.accode  )
			left join  stag_s.stg_tb_core_planspec pp 
			on (accru.plan_code = pp.plan_cd) 
			inner join stag_s.ifrs_common_variable_nm vn
			 on ( vn.event_type = 'Benefit'
			 and vn.variable_cd = 'V1'
			 and vn.benefit_gmm_flg = coa.benefit_gmm_flg  )	
			 left outer join dds.tl_acc_policy_chg pol  
			 on ( accru.policy_no = pol.policy_no
			 and  accru.plan_code = pol.plan_cd
			 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
			 where accru.doc_dt between  v_control_start_dt and v_control_end_dt ;
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP12 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP12 % : % row(s) - V1 ACCRUAL_N1',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 	
 	---=================================== V9 บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน ==============================
 	-- 07Feb2023 Narumol W.: Add column plan_cd 
 	-- 14Feb2023 Narumol W.: V9 : Get data snapshot cheque_dec2023
 	-- Oil 08Mar2023 : Add TB, TLP expire cheque V9
 	-- 12May2023 Narumol W.: ITRQ-66052132 : V9 ส่งข้อมูลแบบกลับ Sign & เอาเข้าเฉพาะเดือนปัจจุบัน โดยเอาที่เคยส่งในเดือนก่อนหน้าออก
  	-- 20Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP

	begin
	 	
	 	if v_control_end_dt = '2022-12-31'::Date then 
	 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	 		( post_dt ,accode , nac_amt , posting_amt ,policy_no,plan_cd ,source_filename
			,variable_cd ,variable_nm ,event_type
			,benefit_gmm_flg, benefit_gmm_desc, policy_type, effective_dt, issued_dt
			,ifrs17_channelcode, ifrs17_partnercode, ifrs17_portfoliocode 
			,ifrs17_portid, ifrs17_portgroup, is_duplicate_variable 
			,group_nm,subgroup_nm,subgroup_desc)
			select post_dt ,accode , nac_amt , posting_amt*-1 as posting_amt ,policy_no,plan_cd ,'YTD_cheque' as source_filename
			,variable_cd ,variable_nm ,event_type
			,benefit_gmm_flg, benefit_gmm_desc, policy_type, effective_dt, issued_dt
			,ifrs17_channelcode, ifrs17_partnercode, ifrs17_portfoliocode 
			,ifrs17_portid, ifrs17_portgroup, is_duplicate_variable 
			,'sap_trial_balance' as group_nm , 'Accruedac_current' as subgroup_nm 
			,'บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน' as subgroup_desc 
	 		from dds.ifrs_variable_benefits_ytd_cheque_dec2023;
	 	  
	 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 			raise notice 'STEP12 % : % row(s) - V9 บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 		
	 	
	 		-- TB-( dds.ifrs_cheque_expired_chg + TLP )
 		 	-- 20Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
	 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			( post_dt,accode,posting_amt,source_filename--,group_nm
			,event_type,benefit_gmm_flg,benefit_gmm_desc
			,variable_cd,variable_nm
			,group_nm,subgroup_nm,subgroup_desc)	
			select report_date as post_dt
			, min(trn.accode) as accode
			--,  sum(trn.accumulated_balance) as accumulated_balance , sum(coalesce(cheque_amt,0)) as cheque_amt 
			, sum(trn.accumulated_balance) - sum(coalesce(cheque_amt,0)) as  posting_amt
			, 'TB_Benefit' as source_filename 
			, coa.event_type, coa.benefit_gmm_flg, coa.benefit_gmm_desc
			, coa.dummy_variable_nm   
			, coa.variable_for_policy_missing   
			,'TB-(ifrs_cheque_expired+TLP)' as group_nm 
			, 'Accruedac_current' as subgroup_nm 
			,'บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน' as subgroup_desc 
			from stag_s.stg_ds_ifrs17_cheque_trial_balance trn 
			inner join stag_s.ifrs_common_coa coa
			on ( coa.event_type = 'TB_Benefit'
			and coa.benefit_gmm_flg ='14'
			and coa.accode= trn.accode ) 
			left outer join ( 
			select sum(cheque_amt) as cheque_amt ,chq.accode,chq.control_dt from (
				select accode ,v_control_end_dt as control_dt , sum(posting_amt*-1) as cheque_amt 
				from dds.ifrs_variable_benefits_ytd_cheque_dec2023 c
				group by c.accode -- 1+2+3
				union 
				select a.accode , a.report_date , a.accumulated_balance
				from stag_s.stg_ds_ifrs17_cheque_trial_balance a
				where a.account_name = 'TLP' --TLP
				and a.report_date = v_control_end_dt
				) as chq  
			group by chq.accode,chq.control_dt )aa 
			on ( trn.accode = aa.accode and account_name <> 'TLP') 
			where report_date = v_control_end_dt
			and account_name <> 'TLP'
			group by report_date, coa.event_type, coa.benefit_gmm_flg, coa.benefit_gmm_desc
			, coa.variable_for_policy_missing 
			, coa.dummy_variable_nm  ;	 
		
		 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 			raise notice 'STEP12 % : % row(s) - TB_Benefit expired cheque ณ สิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 		
	 		
	 	elsif v_control_end_dt > '2023-01-01'::Date then
	 	
	 	 	-- 20Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
		 	insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			( post_dt ,accode , nac_amt , posting_amt ,policy_no,plan_cd ,source_filename
			,variable_cd ,variable_nm ,event_type
			,benefit_gmm_flg, benefit_gmm_desc, policy_type, effective_dt, issued_dt
			,ifrs17_channelcode, ifrs17_partnercode, ifrs17_portfoliocode 
			,ifrs17_portid, ifrs17_portgroup, is_duplicate_variable
			,group_nm,subgroup_nm,subgroup_desc)	
			select v_control_end_dt as post_dt
			,natrn.accode
			,natrn.cheque_amt as nac_amt 
			,natrn.cheque_amt*-1 as posting_amt
			,case when  length(natrn.policy_no) > 12 then null else natrn.policy_no end  as policy_no
			,natrn.plan_cd  ,natrn.source_filename
			, case when natrn.plan_cd is not null then coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') else coa.dummy_variable_nm end as variable_cd
			, case when natrn.plan_cd is not null then coalesce (vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') 
							else coa.variable_for_policy_missing end as variable_nm  
			,vn.event_type
			,coa.benefit_gmm_flg,coa.benefit_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			,'ifrs_cheque_expired' as group_nm 
			,'Accruedac_current' as subgroup_nm 
			,'บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน' as subgroup_desc 
			from  dds.ifrs_cheque_expired_chg natrn
			left join  stag_s.stg_tb_core_planspec pp 
			on ( natrn.plan_cd = pp.plan_cd) 
			 left outer join dds.tl_acc_policy_chg pol  
			on ( natrn.policy_no = pol.policy_no 
			and  natrn.plan_cd = pol.plan_cd 
			and  v_control_end_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
			left join stag_s.ifrs_common_coa coa 
			on (  coa.event_type = 'TB_Benefit' 
			and natrn.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn
			 on ( vn.event_type = 'Benefit' 
			 and vn.variable_cd  = 'V9'
			 and vn.benefit_gmm_flg = coa.benefit_gmm_flg )
			where v_control_end_dt between natrn.valid_fr_dttm and natrn.valid_to_dttm;
			
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 			raise notice 'STEP12 % : % row(s) - V9 บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 		
			
		 	--TB-( dds.ifrs_cheque_expired_chg + TLP )	
 		 	-- 20Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
 		 	-- 19Oct2023 Narumol W.: [ITRQ#66104655] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP

			insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			( post_dt,accode,posting_amt,source_filename,group_nm
			,event_type,benefit_gmm_flg,benefit_gmm_desc
			,variable_cd,variable_nm
			,subgroup_nm,subgroup_desc)		
			select report_date as post_dt
			, min(trn.accode) as accode
			--,  sum(trn.accumulated_balance) as accumulated_balance , sum(coalesce(cheque_amt,0)) as cheque_amt 
			, sum(trn.accumulated_balance) - sum(coalesce(cheque_amt,0)) as  posting_amt
			,'TB_Benefit' as source_filename
			,'TB-(ifrs_cheque_expired+TLP)' as group_nm 
			, coa.event_type, coa.benefit_gmm_flg, coa.benefit_gmm_desc
			, coa.dummy_variable_nm   
			, coa.variable_for_policy_missing  -- as variable_cd  
			--,'sap_trial_balance' as group_nm 
			,'Accruedac_current' as subgroup_nm 
			,'บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน' as subgroup_desc 
			from stag_s.stg_ds_ifrs17_cheque_trial_balance trn 
			inner join stag_s.ifrs_common_coa coa
			on ( coa.event_type = 'TB_Benefit'
			and coa.benefit_gmm_flg ='14'
			and coa.accode= trn.accode ) 
			left outer join ( 
			select sum(cheque_amt) as cheque_amt ,chq.accode,chq.control_dt from (
				select accode ,v_control_end_dt as control_dt , sum(cheque_amt*-1) as cheque_amt 
				from dds.ifrs_cheque_expired_chg  c
				--where c.control_dt = v_control_end_dt
				where v_control_end_dt between c.valid_fr_dttm  and c.valid_to_dttm 
				group by c.accode  -- 1+2+3
				union 
				select a.accode , a.report_date , a.accumulated_balance
				from stag_s.stg_ds_ifrs17_cheque_trial_balance a
				where a.account_name = 'TLP' --TLP
				and a.report_date = v_control_end_dt
				) as chq  
			group by chq.accode,chq.control_dt )aa 	
			on ( trn.accode = aa.accode and account_name <> 'TLP')
			where report_date = v_control_end_dt
			and account_name <> 'TLP'
			group by report_date, coa.event_type, coa.benefit_gmm_flg, coa.benefit_gmm_desc
			, coa.variable_for_policy_missing 
			, coa.dummy_variable_nm  ;	
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 			raise notice 'STEP12 % : % row(s) - TB_Benefit expired cheque ณ สิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 		 
 		end if;
 	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 		raise notice 'STEP12 % : % row(s) - V9 บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 		
 	
 		-- TLP
 		-- 25Apr2023 Narumol W.: Update variable_cd to dummy_variable_nm
 		-- 19Oct2023 Narumol W.: [ITRQ#66104655] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			( post_dt ,accode , nac_amt , posting_amt ,policy_no,plan_cd ,source_filename
			,variable_cd ,variable_nm ,event_type
			,benefit_gmm_flg, benefit_gmm_desc
			,group_nm,subgroup_nm,subgroup_desc)	
			select  report_date as post_dt--v_control_end_dt as post_dt
			,sdictb.accode
			,sdictb.accumulated_balance as nac_amt 
			,sdictb.accumulated_balance  as posting_amt
			,''  as policy_no
			,'_TLP' as plan_cd  ,'TLP_cheque' as source_filename
			,coa.dummy_variable_nm as variable_cd  
			,coa.variable_for_policy_missing 
			,vn.event_type
			,coa.benefit_gmm_flg,coa.benefit_gmm_desc 
			,'TLP' as group_nm 
			,'Accruedac_current' as subgroup_nm
			,'บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน' as subgroup_desc 
			from stag_s.stg_ds_ifrs17_cheque_trial_balance sdictb 
			left join stag_s.ifrs_common_coa coa 
			on (  coa.event_type = 'TB_Benefit' 
			and sdictb.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn
			on ( vn.event_type = 'Benefit' 
			and vn.variable_cd  = 'V9'
			and vn.benefit_gmm_flg = coa.benefit_gmm_flg )
			where account_name ='TLP' 
			and report_date = v_control_end_dt;
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP12 % : % row(s) - TLP ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
        
		  	EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
					,'ERROR STEP12 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;  
				
	END;	 
 	raise notice 'STEP12 % : % row(s) - V9 บันทึกค้างจ่าย expired cheque ณ สิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
  	-- =================================== SUMMARY OF stga_ifrs_nac_txn_step04 ======================================================== --
	drop table if exists stag_a.stga_ifrs_nac_txn_step04_01;
	create table stag_a.stga_ifrs_nac_txn_step04_01 tablespace tbs_stag_a as
	--truncate table stag_a.stga_ifrs_nac_txn_step04_01;
	--insert into stag_a.stga_ifrs_nac_txn_step04_01 
	select ref_1,natrn_x_dim_rk,sap_doc_type,reference_header,post_dt
	,branch_cd,doc_dt,doc_type,doc_no,dc_flg,accode ,is_reverse ,natrn_dim_txt
	,sum(posting_natrn_amt) as posting_natrn_amt ,posting_sap_amt,posting_proxy_amt 
	,detail
	,for_branch_cd
	,event_type 
	,premium_gmm_flg ,premium_gmm_desc
	,claim_gmm_flg,claim_gmm_desc
	,benefit_gmm_flg,benefit_gmm_desc
	,groupeb_gmm_flg,groupeb_gmm_desc 	
	from stag_a.stga_ifrs_nac_txn_step04
	where event_type = 'Benefit'
	group by ref_1,natrn_x_dim_rk,sap_doc_type,reference_header,post_dt
	,branch_cd,doc_dt,doc_type,doc_no,dc_flg,accode ,is_reverse ,natrn_dim_txt,posting_sap_amt,posting_proxy_amt 
	,detail,for_branch_cd,event_type 
	,premium_gmm_flg ,premium_gmm_desc
	,claim_gmm_flg,claim_gmm_desc
	,benefit_gmm_flg,benefit_gmm_desc
	,groupeb_gmm_flg,groupeb_gmm_desc ;
 
	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 	raise notice 'STEP13 % : % row(s) - SUMMARY OF stga_ifrs_nac_txn_step04_01',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
 	-- =================================== SUMMARY OF stga_ifrs_nac_txn_step04 ======================================================== --
/*	drop table if exists stag_a.stga_ifrs_nac_txn_step04_02;
	create table stag_a.stga_ifrs_nac_txn_step04_02 tablespace tbs_stag_a as 
	select ref_1,sap_doc_type,reference_header,post_dt,natrn_x_dim_rk
	,branch_cd,doc_dt,doc_type,doc_no,dc_flg,accode ,is_reverse ,natrn_dim_txt
	,sum(posting_natrn_amt) as posting_natrn_amt ,posting_sap_amt,posting_proxy_amt 
	,detail
	,natrn.sales_id,natrn.sales_struct_n,natrn.selling_partner,natrn.distribution_mode
	,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
	,for_branch_cd
	,event_type  
	,benefit_gmm_flg,benefit_gmm_desc 
	from stag_a.stga_ifrs_nac_txn_step04 natrn
	where event_type = 'Benefit'
	group by ref_1,sap_doc_type,reference_header,post_dt,natrn_x_dim_rk
	,branch_cd,doc_dt,doc_type,doc_no,dc_flg,accode ,is_reverse ,natrn_dim_txt,posting_sap_amt,posting_proxy_amt 
	,detail,natrn.sales_id,natrn.sales_struct_n,natrn.selling_partner,natrn.distribution_mode
	,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center,for_branch_cd,event_type  
	,benefit_gmm_flg,benefit_gmm_desc ;
 
	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	CREATE INDEX  stga_ifrs_nac_txn_step04_02_idx ON stag_a.stga_ifrs_nac_txn_step04_02 USING btree ( doc_dt,doc_no,accode,dc_flg);

	raise notice 'STEP14 % : % row(s) - SUMMARY OF stga_ifrs_nac_txn_step04_02',clock_timestamp()::varchar(19),v_affected_rows::varchar;
*/
/* ย้ายไปอยู่หลัง V7 nac Log.246
	-- =================================== V5 บันทึกจ่ายเงินผลประโยชน์ (Coupon) - แบบฝาก มีดอกเบี้ย ======================================================== --

	-- COUPON-INTEREST
	BEGIN	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits    
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk   
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		--, paycmast.paycmast_amt  as nac_amt 
		--,case when  nac.dc_flg= acc.dc_flg then paycmast.paycmast_amt *inflow_flg else  paycmast.paycmast_amt *inflow_flg*-1 end  as posting_amt
		,nac.amount as nac_amt , nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		--,paycmast.pay_dt 
		,nac.pay_dt
		,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-coupon_paid'::varchar(100) as  group_nm , 'COUPON-INTEREST'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์-แบบฝาก' ::varchar(100) as subgroup_desc  
		,vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04_01  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		)    		
		--inner join stag_a.stga_ifrs_coupon_paid_summary  paycmast 
		--on ( nac.policy_no = paycmast.policy_no 
			--and nac.pay_dt = paycmast.pay_dt  
			--and paycmast.is_interest = 1 ) 
		left join  stag_s.ifrs_common_accode acc  
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn  
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V5'
		 and vn.benefit_gmm_flg= natrn.benefit_gmm_flg  
		 )	
		-- where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk)  
		where  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020' and cc.group_nm = 'natrn-nac-coupon_paid' and cc.policy_no  = nac.policy_no ) 
		and natrn.post_dt between v_control_start_dt and v_control_end_dt; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP15 V5 Coupon have interest : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
	raise notice 'STEP15 % : % row(s) - V5 Coupon have interest',clock_timestamp()::varchar(19),v_affected_rows::varchar;
*/
	-- =================================== V6 บันทึกจ่ายเงินผลประโยชน์ (Dividend) - แบบฝาก มีดอกเบี้ย ======================================================== --
/*	-- DIVIDEND-INTEREST ย้ายไปอยู่หลัง V7
	BEGIN	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits    
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk   
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		--, dv.dvmast_amt  as nac_amt 
		--,case when  nac.dc_flg= acc.dc_flg then dv.dvmast_amt*inflow_flg else dv.dvmast_amt*inflow_flg*-1 end  as posting_amt
		,nac.amount as nac_amt  ,nac.posting_amount  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,dv.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-dividend_paid'::varchar(100) as  group_nm , 'DIVIDEND-INTEREST'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์-แบบฝาก' ::varchar(100) as subgroup_desc  
		,vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04_01  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)    		
		inner join stag_a.stga_ifrs_dividend_paid_summary dv  
		on ( nac.policy_no = dv.policy_no 
			and nac.pay_dt = dv.pay_dt  
			and dv.is_interest = 1 ) 
		left join  stag_s.ifrs_common_accode acc  
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn  
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V6'
		 and natrn.event_type = vn.event_type 
	 	 and vn.benefit_gmm_flg= natrn.benefit_gmm_flg  
		 )	
 		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		where  natrn.post_dt between v_control_start_dt and v_control_end_dt; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP16 V6 Dividend have interest : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
	raise notice 'STEP16 % : % row(s) - V6 Dividend have interest',clock_timestamp()::varchar(19),v_affected_rows::varchar;
*/
	-- ===================================  V7 บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้=========================== -- 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Interest of Deposit'::varchar(100) as subgroup_nm , 'บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้' ::varchar(100) as subgroup_desc  
		,vn.variable_cd, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type
		 and vn.variable_cd = 'V7'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
 		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where  natrn.post_dt  between v_control_start_dt and v_control_end_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP17 V7 Interest of Deposit : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP17 % : % row(s) - V7 Interest of Deposit',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- =================================== V5 บันทึกจ่ายเงินผลประโยชน์ (Coupon) - แบบฝาก มีดอกเบี้ย ======================================================== --

	-- COUPON-INTEREST
	BEGIN	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits    
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk   
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		--, paycmast.paycmast_amt  as nac_amt 
		--,case when  nac.dc_flg= acc.dc_flg then paycmast.paycmast_amt *inflow_flg else  paycmast.paycmast_amt *inflow_flg*-1 end  as posting_amt
		,nac.amount as nac_amt , nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		--,paycmast.pay_dt 
		,nac.pay_dt
		,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-coupon_paid'::varchar(100) as  group_nm , 'COUPON-INTEREST'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์-แบบฝาก' ::varchar(100) as subgroup_desc  
		,vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04_01  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk   -- Log.247
		)    		
		--inner join stag_a.stga_ifrs_coupon_paid_summary  paycmast 
		--on ( nac.policy_no = paycmast.policy_no 
			--and nac.pay_dt = paycmast.pay_dt  
			--and paycmast.is_interest = 1 )  Log.246
		left join  stag_s.ifrs_common_accode acc  
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn  
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V5'
		 and vn.benefit_gmm_flg= natrn.benefit_gmm_flg 
		 -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD 
		 -- 07Dec2023 Narumol W. : Benefit Log 408
		 -- and natrn.benefit_gmm_flg  = '18'
		 )	
		-- 07Dec2023 Narumol W. : Benefit Log 408
		where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk and bb.source_filename  = nac.filename)  
		--where  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc where cc.accode = '5031020020' and cc.group_nm = 'natrn-nac' and cc.policy_no  = nac.policy_no ) -- Log.246
		and natrn.post_dt between v_control_start_dt and v_control_end_dt; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP15 V5 Coupon have interest : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
	raise notice 'STEP15 % : % row(s) - V5 Coupon have interest',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- =================================== V6 บันทึกจ่ายเงินผลประโยชน์ (Dividend) - แบบฝาก มีดอกเบี้ย ======================================================== --
	-- DIVIDEND-INTEREST 
	BEGIN	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits    
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk   
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype  
		,nac.amount as nac_amt  ,nac.posting_amount  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-dividend_paid'::varchar(100) as  group_nm , 'DIVIDEND-INTEREST'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์-แบบฝาก' ::varchar(100) as subgroup_desc  
		,vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04_01  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  
		)    		 
		left join  stag_s.ifrs_common_accode acc  
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn  
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V6'
		 and natrn.event_type = vn.event_type 
	 	 and vn.benefit_gmm_flg= natrn.benefit_gmm_flg  
		 )	
 		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 -- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition : Ignore filter exists in 5031020030
		 where natrn.benefit_gmm_flg = '19' 
		and natrn.post_dt between v_control_start_dt and v_control_end_dt; 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP16 V6 Dividend have interest : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
	raise notice 'STEP16 % : % row(s) - V6 Dividend have interest',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	BEGIN	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits    
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk   
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		--, dv.dvmast_amt  as nac_amt 
		--,case when  nac.dc_flg= acc.dc_flg then dv.dvmast_amt*inflow_flg else dv.dvmast_amt*inflow_flg*-1 end  as posting_amt
		,nac.amount as nac_amt  ,nac.posting_amount  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac-dividend_paid'::varchar(100) as  group_nm , 'DIVIDEND-INTEREST'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์-แบบฝาก' ::varchar(100) as subgroup_desc  
		,vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04_01  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)    		
		--inner join stag_a.stga_ifrs_dividend_paid_summary dv  
		--on ( nac.policy_no = dv.policy_no 
			--and nac.pay_dt = dv.pay_dt  
			--and dv.is_interest = 1 ) 
		left join  stag_s.ifrs_common_accode acc  
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn  
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V6'
		 and natrn.event_type = vn.event_type 
	 	 and vn.benefit_gmm_flg= natrn.benefit_gmm_flg  
		 )	
 		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 -- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition : Ignore filter exists in 5031020030
		 where ( exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
		 				  where cc.accode = '5031020030' and cc.group_nm = 'natrn-nac-dividend_paid' and cc.policy_no  = nac.policy_no ) -- Log.246
		 		)
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits tmp where nac.nac_rk = tmp.nac_rk  )
		and natrn.post_dt between v_control_start_dt and v_control_end_dt; 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP16 V6 Dividend have interest : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
	raise notice 'STEP16 % : % row(s) - V6 Dividend have interest',clock_timestamp()::varchar(19),v_affected_rows::varchar;


	-- ===================================  V7 บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้=========================== -- 
	begin
		
		-- ========Prepare transpos 
		truncate table stag_a.stg_tmp_payment_transpos;
		insert into stag_a.stg_tmp_payment_transpos
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk)
		where pay.doc_dt between  v_control_start_dt and v_control_end_dt ;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP17 % : % row(s) - Prepare stag_a.stg_tmp_payment_transpos',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		/*with paytran as (
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk) -- Log.278
		)*/
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no ,paytrans.plan_cd as plan_cd , paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		,paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit-Interest' as subgroup_nm ,'บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,paytrans.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join stag_a.stg_tmp_payment_transpos  paytrans  
		on (  natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  
		and natrn.natrn_x_dim_rk= paytrans.natrn_x_dim_rk)   --Log.278
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on (  paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit'
		 -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
		 and ( vn.variable_cd = 'V7' 
		 	or ( vn.variable_cd = 'V5' and natrn.benefit_gmm_flg ='18') 
		 	-- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition 
		 	or ( vn.variable_cd = 'V6' and natrn.benefit_gmm_flg ='19') 
		 	) 
		 and natrn.benefit_gmm_flg    = vn.benefit_gmm_flg 	) 
		where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP18 V7 Paytrans Premium discount: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP18 % : % row(s) - V7 -Premium discount',clock_timestamp()::varchar(19),v_affected_rows::varchar;	


	-- ===================================  V1 V5 V6 Payment Trans =========================== -- 
 
	begin
		-- V1 - Coupon แบบไม่มีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		/*with paytran as (
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk) -- Log.278
		)*/
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join stag_a.stg_tmp_payment_transpos  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  
		and natrn.natrn_x_dim_rk= paytrans.natrn_x_dim_rk)   --Log.278
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd ,Log 278 กลับมาใช้ plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
							where cc.accode = '5031020020'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
		and natrn.benefit_gmm_flg = '8' -- Log.236
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V1 Paytrans - Coupon แบบไม่มีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
		-- V1 - Dividend แบบไม่มีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits
		/*with paytran as (
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk) -- Log.278
		)*/
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join stag_a.stg_tmp_payment_transpos  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  
		and natrn.natrn_x_dim_rk= paytrans.natrn_x_dim_rk)   --Log.278
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd ,Log 278 กลับมาใช้ plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
						where cc.accode = '5031020030'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V1 Paytrans - Dividend แบบไม่มีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
		-- V1 - ไม่มีดอกเบี้ย Exclude Coupon,Dividend
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		/*with paytran as (
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk) -- Log.278
		)*/
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join stag_a.stg_tmp_payment_transpos  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  
		and natrn.natrn_x_dim_rk= paytrans.natrn_x_dim_rk)   --Log.278
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd ,Log 278 กลับมาใช้ plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg not in ('8','2.1','2.2')
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V1 Paytrans - ไม่มีดอกเบี้ย Exclude Coupon,Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
		-- V5 - Coupon แบบมีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		/*with paytran as (
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk) -- Log.278
		)*/
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join stag_a.stg_tmp_payment_transpos  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  
		and natrn.natrn_x_dim_rk= paytrans.natrn_x_dim_rk)   --Log.278
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd  -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V5'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg in ('8')-- Log.236  Coupon
		and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
						where cc.accode = '5031020020'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V5 Paytrans - Coupon แบบมีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		-- V6 - Dividend แบบมีดอกเบี้ย 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits
		/*with paytran as (
		select natrn.natrn_x_dim_rk ,pay.*
		from dds.ifrs_payment_transpos_chg pay
		left join dds.oic_natrn_chg natrn
		on (pay.natrn_rk = natrn.natrn_rk) -- Log.278
		)*/
 		select natrn.ref_1 as ref_1 ,paytrans.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, paytrans.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, paytrans.doc_dt , paytrans.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, paytrans.refund_amt 
		, paytrans.posting_refund_amt   as posting_amt
		, paytrans.system_type ,paytrans.transaction_type,paytrans.premium_type 
		, paytrans.policy_no as policy_no 
		, case when coalesce(paytrans.plan_cd,'') ='' then pol.plan_cd else paytrans.plan_cd end as plan_cd 
		, paytrans.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		, paytrans.transpos_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'Benefit - BAY' as subgroup_nm ,'จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, paytrans.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		, paytrans.for_branch_cd
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join stag_a.stg_tmp_payment_transpos  paytrans  
		on ( natrn.branch_cd =  paytrans.branch_cd 
		and natrn.doc_dt = paytrans.doc_dt
		and natrn.doc_type =  paytrans.doc_type 
		and natrn.doc_no = paytrans.doc_no 
		and  natrn.dc_flg = paytrans.dc_flg
		and natrn.accode = paytrans.accode 
		and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  
		and natrn.natrn_x_dim_rk= paytrans.natrn_x_dim_rk)   --Log.278
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( paytrans.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( paytrans.policy_no = pol.policy_no 
		and paytrans.plan_cd = pol.plan_cd   -- Log.234 ข้อมูลหลังจากเดือน 3 กลับมาใช้ paytrans.plan_cd = pol.plan_cd 
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V6'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = paytrans.natrn_rk  )
		and natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend5031020030  5031020020
		and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
					where cc.accode = '5031020030'  and cc.group_nm = 'natrn-paytrans' and cc.policy_no  = paytrans.policy_no )
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP10 % : % row(s) - V6 Paytrans - Dividend แบบมีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP10 V6 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP10 % : % row(s) - Paytrans จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 

  	-- ===================================  V7 Reject Payment Trans =========================== -- 
	begin -- Log.148 K.Ball CF to add data source
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  , 'Reject Benefit-Interest' as subgroup_nm ,'Reject จ่ายดอกเบี้ยของผลประโยชน์'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit'
		 -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
		 and ( vn.variable_cd = 'V7' or  ( vn.variable_cd = 'V5' and natrn.benefit_gmm_flg ='18') 
		 -- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition 
		 	or ( vn.variable_cd = 'V6' and natrn.benefit_gmm_flg ='19') ) 
		 and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP19 V7 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP19 % : % row(s) - V7 Reject จ่ายดอกเบี้ยของผลประโยชน์',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg 
	begin -- Log.148 K.Ball CF to add data source
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	   	select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 		
		, reject.reverse_dim_txt as refund_dim_txt
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm 
		, 'paymentapi_reverse Benefit-Interest' as subgroup_nm ,'PAYMENT API Reverse จ่ายดอกเบี้ยของผลประโยชน์'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd  
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit'
		 -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
		 and ( vn.variable_cd = 'V7' or  ( vn.variable_cd = 'V5' and natrn.benefit_gmm_flg ='18') 
		 -- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition 
		 	or ( vn.variable_cd = 'V6' and natrn.benefit_gmm_flg ='19') ) 
		 and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP19 V7 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP19 % : % row(s) - V7 api_reverse จ่ายดอกเบี้ยของผลประโยชน์',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 
 	-- ===================================  Reject Payment Trans =========================== -- 
	-- 10Jun2024 Narumol W. : [ITRQ#67073352] Enhance Variable Benefit - ยกเลิกการหาว่ามีดอกเบี้ยหรือไม่ ให้เข้า V5,V6 ทั้งหมด
 
 	begin -- Log.147 K.Ball CF to add data source
	 	/*
		--=== V1 - Coupon แบบไม่มีดอกเบี้ย  
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		 where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
							where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no = reject.policy_no ) 
		where natrn.benefit_gmm_flg = '8' -- Log.236
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) -  V1 Reject Coupon แบบไม่มีดอกเบี้ย ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
	   --=== V1 - Dividend แบบไม่มีดอกเบี้ย
	    insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
						where bb.nac_rk = reject.natrn_rk  )
		and natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend  
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
						where cc.accode = '5031020030'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) 
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) - V1 Reject Dividend แบบไม่มีดอกเบี้ย ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		*/
	 	
		--แบบไม่มีดอกเบี้ย Exclude 8-Coupon,2.1-Dividend - INS,2.2-Dividend - INV
		-- 08Aug2024 Narumol W. : [ITRQ#67073352] Enhance Variable Benefit - benefit_gmm_flg not in ('8','2.1','2.2') เข้า V1 เหมือนเดิม 
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
		and natrn.benefit_gmm_flg not in ('8','2.1','2.2') 
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) - V1 ไม่มีดอกเบี้ย Exclude Coupon,Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	 
		-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg 
	
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse Benefit - BAY' as subgroup_nm ,'PAYMENT API Reverse จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd  
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V1'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
		and natrn.benefit_gmm_flg not in ('8','2.1','2.2') 
	    and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) - V1 api_reverse ไม่มีดอกเบี้ย Exclude Coupon,Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		-- 10Jun2024 Narumol W. : [ITRQ#67073352] Enhance Variable Benefit - ยกเลิกการหาว่ามีดอกเบี้ยหรือไม่ ให้เข้า V5,V6 ทั้งหมด
		 
	   --=== V5 - Coupon แบบมีดอกเบี้ย  
	   --Oil 07Dec2022 : Log.236  V5 - Coupon แบบมีดอกเบี้ย  
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	   select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V5'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where   not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
	    and  natrn.benefit_gmm_flg = '8'
		--and  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
		--			where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) -- จ่ายพร้อมดอกเบี้ย
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	    raise notice 'STEP11 % : % row(s) - Coupon แบบมีดอกเบี้ย  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

		-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg 
	
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	   select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse Benefit - BAY' as subgroup_nm ,'PAYMENT API Reverse จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd  
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V5'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where   not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
	    and  natrn.benefit_gmm_flg = '8'
		--and  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
		--			where cc.accode = '5031020020'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) -- จ่ายพร้อมดอกเบี้ย
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	    raise notice 'STEP11 % : % row(s) - api_reverse Coupon แบบมีดอกเบี้ย  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   
	   
	    --=== V6 - Dividend แบบมีดอกเบี้ย   
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
	   select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject Benefit - BAY' as subgroup_nm ,'reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V6'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where   not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
	    and  natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		--and  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
		--			where cc.accode = '5031020030'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) -- จ่ายพร้อมดอกเบี้ย
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	    raise notice 'STEP11 % : % row(s) - Dividend แบบมีดอกเบี้ย  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	     
	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg 
	
	   insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel
		, null::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse Benefit - BAY' as subgroup_nm ,'PAYMENT API Reverse จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd  
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V6'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)
		where   not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = reject.natrn_rk  )
	    and  natrn.benefit_gmm_flg in ('2.1','2.2')-- Log.236  Dividend
		--and  exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
		--			where cc.accode = '5031020030'  and cc.group_nm = 'natrn-reject' and cc.policy_no  = reject.policy_no ) -- จ่ายพร้อมดอกเบี้ย
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
	
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	    raise notice 'STEP11 % : % row(s) - api_reverse Dividend แบบมีดอกเบี้ย  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	   
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP11 Reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP11 % : % row(s) - Reject จ่ายครบกำหนดทุกระยะ/ปันผล ผ่าน BAY  ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	   
 
   	-- ===================================  V7 accode 5031020020 ดอกเบี้ยครบกำหนดสัญญาทุกระยะ ที่แฟ้มสินไหม =========================== --  
	BEGIN 	-- Log.165 K.Ball cf add data source adth ดึงรายการจากแฟ้มสินไหมเพิ่ม เนื่องจากพบว่ามี accode 5031020020 ดอกเบี้ยครบกำหนดสัญญาทุกระยะ ที่แฟ้มสินไหม
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , adth.adth_rk  , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,adth.ref_doc_dt doc_dt,natrn.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.claim_amt 
		,adth.posting_claim_amt as posting_sum_nac_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd  ,null::date pay_dt ,null::varchar pay_by_channel
		,null::varchar as pay_period
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as nac_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'claim' as subgroup_nm ,  'ดอกเบี้ยครบกำหนดสัญญาทุกระยะ' as subgroup_desc 
		,vn.variable_cd
		,vn.variable_name as variable_nm  
		,natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04  natrn
		 inner join dds.oic_adth_chg adth   
		 on (   adth.branch_cd = natrn.branch_cd  
			and adth.ref_doc_dt = natrn.doc_dt 
		  	--and adth.doc_type = natrn.doc_type
		 	and adth.ref_doc_no = natrn.doc_no 
		 	and adth.dc_flg = natrn.dc_flg 
		 	and adth.accode = natrn.accode 
			and adth.adth_dim_txt = natrn.natrn_dim_txt  	)
		 left join stag_s.ifrs_common_accode  acc 
		 on ( natrn.accode = acc.account_cd  )
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd) 
		 left outer join dds.tl_acc_policy_chg pol  
		on ( adth.policy_no = pol.policy_no
		 and adth.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit'
		 -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
		 and ( vn.variable_cd = 'V7' or  ( vn.variable_cd = 'V5' and natrn.benefit_gmm_flg ='18') 
		 -- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition 
		 	or ( vn.variable_cd = 'V6' and natrn.benefit_gmm_flg ='19') ) 
		 and natrn.event_type  = vn.event_type
		 and natrn.benefit_gmm_flg  = vn.benefit_gmm_flg 
		  )	
		-- where not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_benefits bb where adth.adth_rk = bb.nac_rk and bb.group_nm = 'natrn-claim') 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP20 V7 ADTH : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP20 % : % row(s) - V7 ADTH',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	-- ================================  NAC Paylink  ===========================================
	 /* Oil 2022Sep27 : V7 บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้(ด/บ ครบกำหนดทุกระยะ + ด/บ เงินปันผลจ่าย)
				      : V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Coupon แบบไม่มีดอกเบี้ย 
				      : V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Dividend แบบไม่มีดอกเบี้ย
				      : V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - แบบไม่มีดอกเบี้ย Exclude 8-Coupon,2.1-Dividend - INS,2.2-Dividend - INV
					  : V5 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Coupon แบบมีดอกเบี้ย
					  : V6 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Dividend แบบไม่มีดอกเบี้ย
	*/
	-- V7 บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้(ด/บ ครบกำหนดทุกระยะ + ด/บ เงินปันผลจ่าย)

	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
        ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
        ,nac.amount as nac_amt 
        ,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
        ,nac.system_type,nac.transaction_type,nac.premium_type
        ,nac.policy_no,nac.plan_cd, nac.rider_cd   
        ,nac.pay_dt ,nac.pay_by_channel
        ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
        ,nac.nac_dim_txt  
        ,nac.filename as source_filename  , 'natrn-acctbranch_nacpaylink'::varchar(100) as  group_nm , 'Interest of Deposit'::varchar(100) as subgroup_nm , 'บันทึกจ่ายดอกเบี้ยของผลประโยชน์ที่ฝากไว้' ::varchar(100) as subgroup_desc  
        ,vn.variable_cd, vn.variable_name as variable_nm 
        , natrn.detail as nadet_detail 
        , nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type 
        ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
        ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
        from  stag_a.stga_ifrs_nac_txn_step04  natrn  
        inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
        on (  natrn.branch_cd = nac.branch_cd
        and natrn.doc_dt = nac.doc_dt 
        and natrn.doc_type = nac.doc_type
        and natrn.doc_no = nac.doc_no 
        and natrn.dc_flg = nac.dc_flg
        and natrn.accode = nac.accode
        and natrn.natrn_dim_txt = nac.nac_dim_txt
        and natrn.natrn_x_dim_rk=nac.natrn_x_dim_rk -- Log.247
        )   
        left join  stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  )
        left join  stag_s.stg_tb_core_planspec pp   
        on ( nac.plan_cd = pp.plan_cd)
        inner join stag_s.ifrs_common_variable_nm vn
        on ( vn.event_type = 'Benefit'
        and natrn.event_type = vn.event_type
        -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
		and ( vn.variable_cd = 'V7' or  ( vn.variable_cd = 'V5' and natrn.benefit_gmm_flg ='18') 
		-- 22Mar2024 Narumol W. : [ITRQ#67031311] Revised Dividend Interest Condition 
		 	or ( vn.variable_cd = 'V6' and natrn.benefit_gmm_flg ='19') ) 
        and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )        
        left outer join dds.tl_acc_policy_chg pol  
        on ( nac.policy_no = pol.policy_no
        and nac.plan_cd = pol.plan_cd 
        and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
        where  natrn.post_dt  between v_control_start_dt and v_control_end_dt; 
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP16 V7 acctbranch_nacpaylink : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP16 % : % row(s) - V7 acctbranch_nacpaylink',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 
	-- V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Coupon แบบไม่มีดอกเบี้ย 

	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits      
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
        ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
        ,nac.amount as nac_amt 
        ,nac.posting_amount as posting_amt-- posting_sum_nac_amt
        ,nac.system_type,nac.transaction_type,nac.premium_type
        ,nac.policy_no,nac.plan_cd, nac.rider_cd 
        ,nac.pay_dt ,nac.pay_by_channel
        ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
        ,nac.nac_dim_txt  
        ,nac.filename as source_filename  , 'natrn-nacpaylink-coupon_paid'::varchar(100) as  group_nm , 'Pay Benefits nacpaylink'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
        , vn.variable_cd ,vn.variable_name  
        , natrn.detail as nadet_detail 
        , nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type 
        ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
        ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
        from  stag_a.stga_ifrs_nac_txn_step04  natrn 
        inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
        on (  natrn.branch_cd = nac.branch_cd
        and natrn.doc_dt = nac.doc_dt 
        and natrn.doc_type = nac.doc_type
        and natrn.doc_no = nac.doc_no 
        and natrn.dc_flg = nac.dc_flg
        and natrn.accode = nac.accode
        and natrn.natrn_dim_txt = nac.nac_dim_txt  
        and natrn.sales_id = nac.sales_id
        and natrn.natrn_x_dim_rk=nac.natrn_x_dim_rk) -- Log.247 
        left join  stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  )
        left join  stag_s.stg_tb_core_planspec pp 
        on ( nac.plan_cd = pp.plan_cd)
        left outer join stag_a.stga_tl_acc_policy pol  
        on ( nac.policy_no = pol.policy_no
        and nac.plan_cd = pol.plan_cd )
        inner join stag_s.ifrs_common_variable_nm vn
        on ( vn.event_type = 'Benefit'
        and vn.variable_cd = 'V1'
        and natrn.event_type = vn.event_type 
        and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )                
        where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk )
        and natrn.benefit_gmm_flg = '8' 
        and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
        				where cc.accode = '5031020020'  and cc.group_nm = 'natrn-acctbranch_nacpaylink' and cc.policy_no  = nac.policy_no ) 
        and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP18 V1 NAC Paylink - Coupon : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP18 % : % row(s) - V1 NAC Paylink- Coupon',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 
	-- V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Dividend แบบไม่มีดอกเบี้ย

	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
        ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
        ,nac.amount as nac_amt 
        ,nac.posting_amount as posting_amt-- posting_sum_nac_amt
        ,nac.system_type,nac.transaction_type,nac.premium_type
        ,nac.policy_no,nac.plan_cd, nac.rider_cd 
        ,nac.pay_dt ,nac.pay_by_channel
        ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
        ,nac.nac_dim_txt  
        ,nac.filename as source_filename  , 'natrn-nacpaylink-dividend_paid'::varchar(100) as  group_nm , 'Pay Benefits nacpaylink'::varchar(100) as subgroup_nm 
        , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
        , vn.variable_cd ,vn.variable_name  
        , natrn.detail as nadet_detail 
        , nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type 
        ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
        ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
        from  stag_a.stga_ifrs_nac_txn_step04  natrn 
        inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
        on (  natrn.branch_cd = nac.branch_cd
        and natrn.doc_dt = nac.doc_dt 
        and natrn.doc_type = nac.doc_type
        and natrn.doc_no = nac.doc_no 
        and natrn.dc_flg = nac.dc_flg
        and natrn.accode = nac.accode
        and natrn.natrn_dim_txt = nac.nac_dim_txt  
        and natrn.sales_id = nac.sales_id
        and natrn.natrn_x_dim_rk=nac.natrn_x_dim_rk) -- Log.247   
        left join  stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  )
        left join  stag_s.stg_tb_core_planspec pp 
        on ( nac.plan_cd = pp.plan_cd)
        left outer join stag_a.stga_tl_acc_policy pol  
        on ( nac.policy_no = pol.policy_no
        and nac.plan_cd = pol.plan_cd )
        inner join stag_s.ifrs_common_variable_nm vn
        on ( vn.event_type = 'Benefit'
        and vn.variable_cd = 'V1'
        and natrn.event_type = vn.event_type 
        and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )                
        where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk )
        and natrn.benefit_gmm_flg in ('2.1','2.2')
        and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
        				where cc.accode = '5031020030' and cc.group_nm = 'natrn-acctbranch_nacpaylink' and cc.policy_no  = nac.policy_no ) 
        and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP19 V1 NAC Paylink - Dividend : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP19 % : % row(s) - V1 NAC Paylink- Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 	
	-- V1 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Dividend แบบไม่มีดอกเบี้ย Exclude 8-Coupon,2.1-Dividend - INS,2.2-Dividend - INV
 	
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
        ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
        ,nac.amount as nac_amt 
        ,nac.posting_amount as posting_amt-- posting_sum_nac_amt
        ,nac.system_type,nac.transaction_type,nac.premium_type
        ,nac.policy_no,nac.plan_cd, nac.rider_cd 
        ,nac.pay_dt ,nac.pay_by_channel
        ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
        ,nac.nac_dim_txt  
        ,nac.filename as source_filename  , 'natrn-nacpaylink'::varchar(100) as  group_nm , 'Pay Benefits nacpaylink'::varchar(100) as subgroup_nm 
        , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
        , vn.variable_cd ,vn.variable_name  
        , natrn.detail as nadet_detail 
        , nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type 
        ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
        ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
        from  stag_a.stga_ifrs_nac_txn_step04  natrn 
        inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
        on (  natrn.branch_cd = nac.branch_cd
        and natrn.doc_dt = nac.doc_dt 
        and natrn.doc_type = nac.doc_type
        and natrn.doc_no = nac.doc_no 
        and natrn.dc_flg = nac.dc_flg
        and natrn.accode = nac.accode
        and natrn.natrn_dim_txt = nac.nac_dim_txt  
        and natrn.sales_id = nac.sales_id
        and natrn.natrn_x_dim_rk=nac.natrn_x_dim_rk)-- Log.247
        left join  stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  )
        left join  stag_s.stg_tb_core_planspec pp 
        on ( nac.plan_cd = pp.plan_cd)
        left outer join stag_a.stga_tl_acc_policy pol  
        on ( nac.policy_no = pol.policy_no
        and nac.plan_cd = pol.plan_cd )
        inner join stag_s.ifrs_common_variable_nm vn
        on ( vn.event_type = 'Benefit'
        and vn.variable_cd = 'V1'
        and natrn.event_type = vn.event_type 
        and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )                
        where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk )
        and natrn.benefit_gmm_flg not in ('8','2.1','2.2') 
        and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP20 V1 NAC Paylink - Dividend แบบไม่มีดอกเบี้ย : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP20 % : % row(s) - V1 NAC Paylink- Dividend แบบไม่มีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 	
	-- V5 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Coupon แบบมีดอกเบี้ย

    BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
        ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
        ,nac.amount as nac_amt 
        ,nac.posting_amount as posting_amt-- posting_sum_nac_amt
        ,nac.system_type,nac.transaction_type,nac.premium_type
        ,nac.policy_no,nac.plan_cd, nac.rider_cd 
        ,nac.pay_dt ,nac.pay_by_channel
        ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
        ,nac.nac_dim_txt  
        ,nac.filename as source_filename  , 'natrn-nacpaylink-coupon_paid'::varchar(100) as  group_nm , 'Pay Benefits nacpaylink'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
        , vn.variable_cd ,vn.variable_name  
        , natrn.detail as nadet_detail 
        , nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type 
        ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
        ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
        from  stag_a.stga_ifrs_nac_txn_step04  natrn 
        inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
        on (  natrn.branch_cd = nac.branch_cd
        and natrn.doc_dt = nac.doc_dt 
        and natrn.doc_type = nac.doc_type
        and natrn.doc_no = nac.doc_no 
        and natrn.dc_flg = nac.dc_flg
        and natrn.accode = nac.accode
        and natrn.natrn_dim_txt = nac.nac_dim_txt  
        and natrn.sales_id = nac.sales_id
        and natrn.natrn_x_dim_rk=nac.natrn_x_dim_rk)-- Log.247
        left join  stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  )
        left join  stag_s.stg_tb_core_planspec pp 
        on ( nac.plan_cd = pp.plan_cd)
        left outer join stag_a.stga_tl_acc_policy pol  
        on ( nac.policy_no = pol.policy_no
        and nac.plan_cd = pol.plan_cd )
        inner join stag_s.ifrs_common_variable_nm vn
        on ( vn.event_type = 'Benefit'
        and vn.variable_cd = 'V5'
        and natrn.event_type = vn.event_type 
        and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )                
        where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk )
        and natrn.benefit_gmm_flg = '8' 
        and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
        			where cc.accode = '5031020020' and cc.group_nm = 'natrn-acctbranch_nacpaylink' and cc.policy_no  = nac.policy_no ) 
        and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP21 V5 NAC Paylink - Coupon แบบมีดอกเบี้ย : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP21 % : % row(s) - V5 NAC Paylink - Coupon แบบมีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 	
   --  V6 NAC Paylink  บันทึกจ่ายเงินผลประโยชน์ - Dividend แบบไม่มีดอกเบี้ย
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
 		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
       ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
       ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
       ,nac.amount as nac_amt 
       ,nac.posting_amount as posting_amt-- posting_sum_nac_amt
       ,nac.system_type,nac.transaction_type,nac.premium_type
       ,nac.policy_no,nac.plan_cd, nac.rider_cd 
       ,nac.pay_dt ,nac.pay_by_channel
       ,null::varchar as pay_period
       ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
       ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
       ,nac.nac_dim_txt  
       ,nac.filename as source_filename  , 'natrn-nacpaylink-dividend_paid'::varchar(100) as  group_nm , 'Pay Benefits nacpaylink'::varchar(100) as subgroup_nm 
       , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
       , vn.variable_cd ,vn.variable_name  
       , natrn.detail as nadet_detail 
       , nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
       , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
       , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
       , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
       ,natrn.for_branch_cd
       ,natrn.event_type 
       ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
       ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
       ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
       ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
       ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
       from  stag_a.stga_ifrs_nac_txn_step04  natrn 
       inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
       on (  natrn.branch_cd = nac.branch_cd
       and natrn.doc_dt = nac.doc_dt 
       and natrn.doc_type = nac.doc_type
       and natrn.doc_no = nac.doc_no 
       and natrn.dc_flg = nac.dc_flg
       and natrn.accode = nac.accode
       and natrn.natrn_dim_txt = nac.nac_dim_txt  
       and natrn.sales_id = nac.sales_id
       and natrn.natrn_x_dim_rk=nac.natrn_x_dim_rk)-- Log.247
       left join  stag_s.ifrs_common_accode acc 
       on ( natrn.accode = acc.account_cd  )
       left join  stag_s.stg_tb_core_planspec pp 
       on ( nac.plan_cd = pp.plan_cd)
       left outer join stag_a.stga_tl_acc_policy pol  
       on ( nac.policy_no = pol.policy_no
       and nac.plan_cd = pol.plan_cd )
       inner join stag_s.ifrs_common_variable_nm vn
       on ( vn.event_type = 'Benefit'
       and vn.variable_cd = 'V6'
       and natrn.event_type = vn.event_type 
       and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )                
       where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk )
       and natrn.benefit_gmm_flg in ('2.1','2.2')
       and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
       				where cc.accode = '5031020030' and cc.group_nm = 'natrn-acctbranch_nacpaylink' and cc.policy_no  = nac.policy_no ) 
       and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
       GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP22 V6 NAC Paylink - Dividend แบบไม่มีดอกเบี้ย : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP22 % : % row(s) - V6 NAC Paylink - Dividend แบบไม่มีดอกเบี้ย',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 	
	-- Oil 20230609 :ITRQ#66052475 ขอปรับเงื่อนไข V2 BENEFIT_YTD สำหรับข้อมูลจาก system type 14,15 
	-- 03Nov2023 Narumol W. : [ITRQ#66104785] ขอปรับปรุงเงื่อนไข Coupon Dividend BENEFIT_YTD

	--=== with APL
	-- '14' as system_type_cd 
	-- '15' as system_type_cd 
	begin
		
		select dds.fn_ifrs_coupon_dividend_nonapl (p_xtr_start_dt) into out_err_cd;
	 	raise notice 'STEP22 % : err_cd = %  - call fn fn_ifrs_coupon_dividend_nonapl',clock_timestamp()::varchar(19),out_err_cd::varchar;	

		if v_control_end_dt >= '2023-07-31'::date then
			  
			-- 03Nov2023 Narumol W. : [ITRQ#66104785] ขอปรับปรุงเงื่อนไข Coupon Dividend BENEFIT_YTD
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			(post_dt,policy_no,plan_cd,posting_amt,source_filename,group_nm
			,subgroup_nm,subgroup_desc,variable_cd,variable_nm,event_type,benefit_gmm_flg
			,product_group ,product_sub_group ,product_term ,rider_group ,cost_center  
			,system_type)
			select control_dt , policy_no ,plan_cd , apl_amt as posting_amt 
			,'ifrs_benefits_ndic_by_policy'::varchar as source_file_name
			, concat('APL Dividend & COUPON ',trim(ndic_dv))::varchar as  group_nm
			,'Accruedac_current'::varchar(100) as subgroup_nm
			,'Dividend Coupon  System types : 14 ,15 ส่งไปที่ PVCF'::varchar  as subgroup_desc 
			,'V2'::varchar as variable_cd  
			, vn.variable_name  as variable_nm 
			,'Benefit'::varchar as event_type 
			,coa.benefit_gmm_flg  as benefit_gmm_flg
			,product_group ,product_sub_group ,product_term ,rider_group ,cost_center  
			,case when coa.benefit_gmm_flg  in ('8','18','19') then '014'  --  Benefit Log. 412
			 	 when coa.benefit_gmm_flg in ('2.1','2.2') then '015' end system_type 
			from dds.ifrs_benefits_ndic_by_policy ndic 
			left outer join stag_s.ifrs_common_coa coa
			on ( coa.event_type ='Benefit' 
			and ndic.accode=coa.accode)
			left outer join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type ='Benefit'
			and  vn.variable_cd = 'V2' 
			and coa.benefit_gmm_flg = vn.benefit_gmm_flg )
			where coalesce(apl_amt,0) <> 0
			and control_dt = v_control_end_dt;
		  
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 		raise notice 'STEP22 % : % row(s) - V2 - Coupon&Dividend Change source to ifrs_benefits_ndic_by_policy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
		else 
		
			select variable_name 
			into v_variable_nm
			from stag_s.ifrs_common_variable_nm
			where event_type ='Benefit'
			and variable_cd = 'V2'
			and benefit_gmm_flg ='2.2'; --15Sep2023 Narumol W.: N'Ked cf to change gmm from 2.1 to 2.2 
			
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			(post_dt,policy_no,plan_cd,posting_amt,system_type,source_filename,group_nm
			,subgroup_nm,subgroup_desc,variable_cd,variable_nm,event_type,benefit_gmm_flg)
			select  v_control_end_dt  as post_dt
			,apl.policy_no , trim(apl.plan_cd) as plan_cd
			,case when sum(coalesce(apl.coupon_amt,0)) != 0 then  sum(apl.coupon_amt)*-1
			      when  sum(coalesce(apl.dividend_amt,0)) != 0 then sum(apl.dividend_amt)*-1  end as posting_amt 
			,case when sum(coalesce(apl.coupon_amt,0)) != 0 then  '014' 
			      when  sum(coalesce(apl.dividend_amt,0)) != 0 then  '015'  end 	as system_type_cd 
			,apl.source_filename  as source_filename 
			,'APL Dividend' as  group_nm
			,'Accruedac_current'::varchar(100) as subgroup_nm
			,'Dividend Coupon  System types : 14 ,15 ส่งไปที่ PVCF'  as subgroup_desc 
			,'V2' as variable_cd  
			, v_variable_nm as variable_nm 
			,'Benefit' as event_type 
			,'2.2' as benefit_gmm_flg
			from  dds.vw_ifrs_apl apl 
			where  coalesce(apl.dividend_amt ,0) > 0 
			group by  apl.policy_no , trim(apl.plan_cd) ,apl.source_filename ;
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 		raise notice 'STEP21 % : % row(s) - V2 - Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	 		select variable_name 
			into v_variable_nm
			from stag_s.ifrs_common_variable_nm
			where event_type ='Benefit'
			and variable_cd = 'V2'
			and benefit_gmm_flg ='8';
			
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			(post_dt,policy_no,plan_cd,posting_amt,system_type,source_filename,group_nm
			,subgroup_nm,subgroup_desc,variable_cd,variable_nm,event_type,benefit_gmm_flg)
			select  v_control_end_dt  as post_dt
			,apl.policy_no , trim(apl.plan_cd) as plan_cd
			,case when sum(coalesce(apl.coupon_amt,0)) != 0 then  sum(apl.coupon_amt)*-1
			      when  sum(coalesce(apl.dividend_amt,0)) != 0 then sum(apl.dividend_amt)*-1  end as posting_amt 
			,case when sum(coalesce(apl.coupon_amt,0)) != 0 then  '014' 
			      when  sum(coalesce(apl.dividend_amt,0)) != 0 then  '015'  end 	as system_type_cd 
			,apl.source_filename  as source_filename 
			,'APL Coupon' as  group_nm
			,'Accruedac_current'::varchar(100) as subgroup_nm
			,'Dividend Coupon  System types : 14 ,15 ส่งไปที่ PVCF'  as subgroup_desc 
			,'V2' as variable_cd  
			, v_variable_nm as variable_nm 
			,'Benefit' as event_type 
			,'8' as benefit_gmm_flg
			from  dds.vw_ifrs_apl apl 
			where  coalesce(apl.coupon_amt,0)  > 0 
			group by  apl.policy_no , trim(apl.plan_cd) ,apl.source_filename ;
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 		raise notice 'STEP21 % : % row(s) - V2 - Coupon',clock_timestamp()::varchar(19),v_affected_rows::varchar;		
	 	
		end if; 
 	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP21 V6 COUPON ACCRUAL-NO INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP21 % : % row(s) - V2 - TERMINATE to PVCF',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- 26June2023 Narumol W. : Edit condition  
  	-- 16Oct2023 Narumol W. : Use valid_val_fr_dttm in PVCF condition

	BEGIN
		--=== IF
		delete from stag_a.stga_ifrs_apl_if 
		where  control_dt = v_control_end_dt ;
		
		insert into stag_a.stga_ifrs_apl_if  
		--create table stag_a.stga_ifrs_apl_if  as  
		with accru as (
		select accru.policy_no,trim(accru.plan_code) as plan_code ,accru.system_type_cd,sum(accru.posting_accru_amount) as accru_amt
		,case when pol.policy_status_cd in ('I','F','B','N','A','E','R','U') then 'IF' else 'Terminate' end as pol_status_cd
		from dds.vw_ifrs_accrual_chg accru  --oil :20230616 : all accru
		left join dds.tl_acc_policy_chg pol   
		on ( accru.policy_no = pol.policy_no
		and  trim(accru.plan_code) = trim(pol.plan_cd)
		and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
		where accru.system_type_cd in ('014','015')  
		and exists ( select 1 from stag_s.ifrs_common_coa coa  
 			where  coa.event_type = 'Benefit'
			 and coa.benefit_gmm_flg in ( '1','2.1','2.2','8','18','19') --  Benefit Log. 412
			 and accru.accode  = coa.accode )
		group by accru.policy_no,trim(accru.plan_code),accru.system_type_cd 
		,case when pol.policy_status_cd in ('I','F','B','N','A','E','R','U') then 'IF' else 'Terminate' end  

		)  
		, apl as (  
		select  v_control_end_dt  as post_dt ,
		apl.policy_no , trim(apl.plan_cd) as  plan_cd ,sum(apl.coupon_amt)*-1 as coupon_amt ,sum(apl.dividend_amt)*-1 as dividend_amt
		,case when coalesce(apl.coupon_amt,0) != 0 then  '014' 
		      when  coalesce(apl.dividend_amt,0) != 0 then  '015'  end 	as system_type_cd 
		from  dds.vw_ifrs_apl apl 
		where coalesce(apl.coupon_amt,0) + coalesce(apl.dividend_amt ,0) > 0 
		group by apl.policy_no , trim(apl.plan_cd),system_type_cd
		) 
		
		select accru.system_type_cd , accru.policy_no,accru.plan_code ,pol_status_cd
		,accru.accru_amt 
		,apl.coupon_amt
		,apl.dividend_amt
		-- 27Dec2023 Narumol W. : fix bug wheb coupon_amt is not null 
		--,(accru.accru_amt- coalesce(coalesce(apl.coupon_amt,apl.dividend_amt),0))  as posting_amt_diff
		,accru.accru_amt - (coalesce(apl.coupon_amt,0) + coalesce(apl.dividend_amt ,0)) as posting_amt_diff 
		, v_control_end_dt as control_dt  
		from accru 
		left join apl apl
		on accru.policy_no= apl.policy_no
		and accru.plan_code = apl.plan_cd 
		and accru.system_type_cd = apl.system_type_cd;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP21 IF : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP21 % : % row(s) - IF : 14 ,15',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	--=== non APL 
	--Terminate system_type_cd in ('014') 
	--Terminate system_type_cd in ('015') 
 	-- 26June2023 Narumol W. : Edit condition 
	BEGIN
	 	insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		(post_dt,policy_no,plan_cd,system_type,posting_amt,source_filename,group_nm
		,subgroup_nm,subgroup_desc,variable_cd,variable_nm,event_type) 
		select control_dt,policy_no ,plan_code ,system_type_cd ,posting_amt_diff
		,'actuary_apl'  as source_filename 
		,'Non APL' as  group_nm
		,'Accruedac_current'::varchar(100) as subgroup_nm
		,'NON APL Terminate system_type_cd : 14 ,15 ส่งไปที่ PVCF'  as subgroup_desc 
		,'PVCF' as variable_cd  
		,'PVCF' as variable_nm 
		,'Benefit' as event_type 
		from stag_a.stga_ifrs_apl_if 
		where pol_status_cd = 'Terminate';
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		
		/*with accru as (
		select accru.policy_no,trim(accru.plan_code)as plan_code ,accru.system_type_cd,sum(accru.posting_accru_amount) as accru_amt
		,case when pol.policy_status_cd in ('I','F','B','N','A','E','R','U','?') then 'IF' else 'Terminate' end as pol_status_cd
		from dds.vw_ifrs_accrual_chg accru 
		left join dds.tl_acc_policy_chg pol   --oil :20230616 : all accru
		on ( accru.policy_no = pol.policy_no
		and  trim(accru.plan_code) = trim(pol.plan_cd)
		and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm  ) 
		--and pol.policy_status_cd not in ('I','F','B','N','A','E','R','U','?') )
		where  accru.system_type_cd in ('014','015')  
		and exists ( select 1 from stag_s.ifrs_common_coa coa  
 			where  coa.event_type = 'Benefit'
			 and coa.benefit_gmm_flg in ( '1','2.1','2.2','8')
			 and accru.accode  = coa.accode )
		group by accru.policy_no,trim(accru.plan_code),accru.system_type_cd
		)  
		, apl as (  
		select  
		apl.policy_no , trim(apl.plan_cd) as plan_cd,sum(apl.coupon_amt) as coupon_amt ,sum(apl.dividend_amt) as dividend_amt
		,case when coalesce(apl.coupon_amt,0) != 0 then  '014' 
		      when  coalesce(apl.dividend_amt,0) != 0 then  '015'  end 	as system_type_cd 
		from  dds.vw_ifrs_apl apl 
		where coalesce(apl.coupon_amt,0) + coalesce(apl.dividend_amt ,0) > 0 
		group by apl.policy_no , trim(apl.plan_cd),system_type_cd
		)
		select v_control_end_dt  as post_dt ,
		accru.policy_no,accru.plan_code,accru.system_type_cd 
		,(accru.accru_amt- coalesce(coalesce(apl.coupon_amt,apl.dividend_amt),0))  as posting_amt_diff
		,'actuary_apl'  as source_filename 
		,'Non APL' as  group_nm
		,'Accruedac_current'::varchar(100) as subgroup_nm
		,'NON APL Terminate system_type_cd : 14 ,15 ส่งไปที่ PVCF'  as subgroup_desc 
		,'PVCF' as variable_cd  
		,'PVCF' as variable_nm 
		,'Benefit' as event_type 
		from accru 
		left join apl apl
		on accru.policy_no= apl.policy_no
		and accru.plan_code = apl.plan_cd 
		and accru.system_type_cd = apl.system_type_cd;
 		*/
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP21 NON APL Terminate : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP21 % : % row(s) - NON APL Terminate system_type_cd : 14 ,15',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- ===================================  COUPON ===== ====================== -- 
	-- COUPON ACCRUAL - NO INTEREST  
	--1) เป็นรายการ Coupon ที่มี status เป็น Inforce with APL (from actuarial) = deposit (ตั้งค้างจ่ายคูปอง,เงินปันผล และตั้ง APL ไม่มีดอกเบี้ยคูปองและเงินปันผล แค่รอ settle) System types : 28
	--2) เป็นรายการ Coupon ที่มี status เป็น Terminate  System types : 14 ส่งไปที่ PVCF oil : ยกเลิก ITRQ#66052475
	--3) system types 33 ถ้าเป็น Policy ที่มีดอกเบี้ยจะส่งไปให้ Actuary ใน PVCF เหมือน Terminate  - ไม่ต้องเช็ค status terminate
	--4) system types 33 ถ้าเป็น Policy ที่ไม่มีดอกเบี้ยจะอยู่ที่ Var.นี้ (ดูภายในเดือนนั้นๆ) - ไม่ต้องเช็ค status  terminate
	 
	-- 1) เป็นรายการ Coupon ที่มี status เป็น Inforce with APL (from actuarial) = deposit (ตั้งค้างจ่ายคูปอง,เงินปันผล และตั้ง APL ไม่มีดอกเบี้ยคูปองและเงินปันผล แค่รอ settle) System types : 28
   	-- 16Oct2023 Narumol W. : Use valid_val_fr_dttm in PVCF condition

	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits      
		select  natrn.ref_1 as ref_1, accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header , natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt 
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-coupon_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm  -- APL- Coupon
		, 'Coupon ที่มี status เป็น Inforce with APL (from actuarial)'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no  as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V2'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)	
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 and natrn.benefit_gmm_flg = '8'  
		 and accru.system_type_cd = '028'  
		 and exists ( select 1 from  dds.vw_ifrs_apl apl 
		 				where accru.policy_no = apl.policy_no 
		 				and apl.coupon_amt > 0 ) 
		 				--and apl.valst_status_cd in ('I','F','B','N')  ) --20Jun22 Bella cf to get all status 
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
							where bb.nac_rk = accru.accrual_rk 
							and bb.source_filename = 'accrued-ac' ); 

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP21 V6 COUPON ACCRUAL-NO INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP21 % : % row(s) - V6 COUPON ACCRUAL-NO INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar;
-- Oil 20230609 :ITRQ#66052475 ขอปรับเงื่อนไข V2 BENEFIT_YTD สำหรับข้อมูลจาก system type 14,15 
 /*
 	-- 2) เป็นรายการ Coupon ที่มี status เป็น Terminate  System types : 14 ส่งไปที่ PVCF 
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1,accru.accrual_rk  as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt 
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-coupon_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm -- Outstanding Coupon status Terminate - PVCF
		, 'Coupon ที่มี status เป็น Terminate  System types : 14 ส่งไปที่ PVCF'  as subgroup_desc 
		-- vn.variable_cd , vn.variable_name
		, 'PVCF' as variable_cd , 'PVCF' as variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,0::int as is_duplicate_variable ,null::varchar as duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_chg accru 
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd) 
		inner join stag_a.stga_ifrs_coupon_unpaid_summary paycmast  
		on ( accru.policy_no = paycmast.policy_no  ) 
		/*
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type
		 and vn.variable_cd = 'V2'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)*/
		 inner join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm  
		 and pol.policy_status_cd not in ('I','F','B','N','A','E','R','U','?','J') )
		 --and pol.is_active = 0) -- Log.102 
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt 
		 and accru.system_type_cd in (  '014' )
		 and natrn.benefit_gmm_flg in ( '8','1');
		-- and accru.accode in ( '5031010020','5031020020') ;     
		  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP22 V2 COUPON ACCRUAL- TERMINATE to PVCF : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP22 % : % row(s) - V2 COUPON ACCRUAL- TERMINATE to PVCF',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 */
 	-- =================================== V2 2) เป็นรายการ Coupon แบบฝากแต่ status เป็น Terminate  ======================================================== --
  	-- 3) system types 33 ถ้าเป็น Policy ที่มีดอกเบี้ยจะส่งไปให้ Actuary ใน PVCF เหมือน Terminate  - ไม่ต้องเช็ค status terminate
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		 select  natrn.ref_1 as ref_1,accru.accrual_rk  as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt 
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		, null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-coupon_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm --Outstanding Coupon with interest - PVCF
		, 'Coupon แบบฝากมีดอกเบี้ย ส่งไปที่ PVCF'  as subgroup_desc  
		, 'PVCF' as variable_cd , 'PVCF' as variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,0::int as is_duplicate_variable ,null::varchar as duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_chg accru 
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd)
		-- 04Aug2022 NarumolW. ChgNo.38  : Change condition to Lookup accode interest
		--inner join stag_a.stga_ifrs_coupon_unpaid_summary paycmast  
		--on ( accru.policy_no = paycmast.policy_no  and paycmast.is_interest = 1 )  
		 inner join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm ) 
		-- and pol.is_active = 0) -- Log.102 ดูเฉพาะ มีดอกเบี้ย แต่ไม่ต้องกรอง Terminate 
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt 
		 and accru.system_type_cd in (  '033' )  
		 and natrn.benefit_gmm_flg in ( '8','1','18')  --  Benefit Log. 412 
		 and exists ( select 1 from dds.vw_ifrs_accrual_chg  b
		 				where accode = '5031020020'  
		 				and accru.policy_no  = b.policy_no  
		 				and accru.system_type_cd = b.system_type_cd);  -- ChgNo.38 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP23 V2 COUPON ACCRUAL- INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP23 % : % row(s) - V2 COUPON ACCRUAL-  INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	-- 4) system types 33 ถ้าเป็น Policy ที่ไม่มีดอกเบี้ยจะอยู่ที่ Var.นี้ (ดูภายในเดือนนั้นๆ) - ไม่ต้องเช็ค status  terminate
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1,accru.accrual_rk  as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt
		--, paycmast.pcunpaid_amt as nac_amt 
 		--, case when  accru.dc_flg= acc.dc_flg then paycmast.pcunpaid_amt*inflow_flg else paycmast.pcunpaid_amt*inflow_flg*-1 end  as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-coupon_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm --Outstanding Coupon (ส่วนเพิ่ม) without interest
		, 'Coupon ที่ไม่มีดอกเบี้ย ส่งเข้า V2 เฉพาะเดือนปัจจุบัน'  as subgroup_desc 
		, vn.variable_cd , vn.variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru 
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd)
		-- 04Aug2022 NarumolW. ChgNo.38  : Change condition to Lookup accode interest
		--left join stag_a.stga_ifrs_coupon_unpaid_summary paycmast  
		--on ( accru.policy_no = paycmast.policy_no and paycmast.is_interest <> 1 ) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type
		 and vn.variable_cd = 'V2'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)	
		 inner join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
		-- and pol.is_active = 0) -- Log.102 ดูเฉพาะ ไม่มีดอกเบี้ย แต่ไม่ต้องกรอง Terminate 
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 and natrn.benefit_gmm_flg in ( '8','18') --  Benefit Log. 412
		 and accru.system_type_cd in (  '033' ) --in ( '014','033','028')  
		 and not exists ( select 1 from dds.vw_ifrs_accrual_chg  b
		 				where accode = '5031020020'  
		 				and accru.policy_no  = b.policy_no
		 				and accru.system_type_cd = b.system_type_cd)  -- ChgNo.38
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
							where bb.nac_rk = accru.accrual_rk 
							and source_filename = 'accrued-ac'); 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP24 V2 COUPON ACCRUAL- INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP24 % : % row(s) - V2 COUPON ACCRUAL-  INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- ===================================================== V2 DIVIDEND ============================================================================ --  
	/*
	 1) เป็นรายการ Dividend ที่มี status เป็น Inforce with APL (from actuarial) = deposit (ตั้งค้างจ่ายคูปอง,เงินปันผล และตั้ง APL ไม่มีดอกเบี้ยคูปองและเงินปันผล แค่รอ settle) System types : 29
	 2) เป็นรายการ Dividend ที่มี status เป็น Terminate System types : 15 ส่งไปที่ PVCF oil : ยกเลิก ITRQ#66052475
	 3) system types 34 ถ้าเป็น Policy ที่มีดอกเบี้ยจะส่งไปให้ Actuary ใน PVCF เหมือน Terminate  - ไม่ต้องเช็ค status  terminate
	 4) system types 34 ถ้าเป็น Policy ที่ไม่มีดอกเบี้ยจะอยู่ที่ Var.นี้ (ดูภายในเดือนนั้นๆ)  - ไม่ต้องเช็ค status  terminate
	*/	
 	-- 1) เป็นรายการ Dividend ที่มี status เป็น Inforce with APL (from actuarial) = deposit (ตั้งค้างจ่ายคูปอง,เงินปันผล และตั้ง APL ไม่มีดอกเบี้ยคูปองและเงินปันผล แค่รอ settle)  System types : 29  
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header , natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type ,accru.accru_amt , accru.posting_accru_amount as posting_amt 
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n
		, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-dividend_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm --APL- Dividend
		, 'Dividend ที่มี status เป็น APL (from actuarial)'  as subgroup_desc 
		, vn.variable_cd , vn.variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type
		 and vn.variable_cd = 'V2'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)			
		left join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd 
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm  )
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 and natrn.benefit_gmm_flg in ( '2.1','2.2')
		 and accru.system_type_cd  = '029' 
		 and exists ( select 1 from dds.vw_ifrs_apl apl 
		 				where accru.policy_no = apl.policy_no 
		 				and apl.dividend_amt > 0 )
		 				--and apl.valst_status_cd in ('I','F','B','N')  ) --20Jun22 Bella cf to get all status 
		  and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
							where bb.nac_rk = accru.accrual_rk 
							and source_filename = 'accrued-ac'); 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP25 V2 DIVIDEND ACCRUAL-NO INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP25 % : % row(s) - V2 DIVIDEND ACCRUAL-NO INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 -- Oil 20230609 :ITRQ#66052475 ขอปรับเงื่อนไข V2 BENEFIT_YTD สำหรับข้อมูลจาก system type 14,15 
 /* 
 	--  2) เป็นรายการ Dividend ที่มี status เป็น Terminate System types : 15 ส่งไปที่ PVCF
  	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type  
 		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-dividend_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm --Outstanding Dividend status Termicate - PVCF
		, 'Dividend ที่มี status เป็น Terminate'  as subgroup_desc 
		, 'PVCF' as variable_cd , 'PVCF' as variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,0::int as is_duplicate_variable ,null::varchar as duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_chg accru   
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd) 
		inner join stag_a.stga_ifrs_dividend_unpaid_summary dvmast  
		on ( accru.policy_no = dvmast.policy_no ) 
		inner join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd 
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm  
		 and pol.policy_status_cd not in ('I','F','B','N','A','E','R','U','?','J'))
		 --and pol.is_active = 0) 
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 --and natrn.benefit_gmm_flg in ('2.1','1')
		 and accru.system_type_cd in ( '015' ) 
		 and accru.accode in ( '5031010030','5031020030'); 
 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP26 V2 DIVIDEND ACCRUAL- INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP26 % : % row(s) - V2 DIVIDEND ACCRUAL-  INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
  */
 	--3) system types 34 ถ้าเป็น Policy ที่มีดอกเบี้ยจะส่งไปให้ Actuary ใน PVCF เหมือน Terminate  - ไม่ต้องเช็ค status  terminate
 	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type  
 		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-dividend_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm --Outstanding Dividend (ส่วนเพิ่ม) with interest - PVCF
		, 'Dividend ที่มีดอกเบี้ยจะส่งไปให้ Actuary ใน PVCF'  as subgroup_desc 
		, 'PVCF' as variable_cd , 'PVCF' as variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,0::int as is_duplicate_variable ,null::varchar as duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_chg accru   
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd 
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
		-- and pol.is_active = 0)-- Log.102 ดูเฉพาะ ไม่มีดอกเบี้ย แต่ไม่ต้องกรอง Terminate 
		-- 04Aug2022 NarumolW. ChgNo.38  : Change condition to Lookup accode interest 
		--inner join stag_a.stga_ifrs_dividend_unpaid_summary paycmast  
		--on ( accru.policy_no = paycmast.policy_no  and paycmast.is_interest = 1 )  
		where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		and natrn.benefit_gmm_flg in ('2.1','2.2','1')
		and accru.system_type_cd in ( '034' )
		--and accru.accode in ( '5031010030' )   
		and exists ( select 1 from dds.vw_ifrs_accrual_chg   b 
		 			where b.accode = '5031020030'  
		 			and accru.policy_no  = b.policy_no
		 			and accru.system_type_cd = b.system_type_cd);  -- ChgNo.38
					
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP27 V2 DIVIDEND ACCRUAL- INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP27 % : % row(s) - V2 DIVIDEND ACCRUAL-  INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar; 

 	--4) system types 34 ถ้าเป็น Policy ที่ไม่มีดอกเบี้ยจะอยู่ที่ Var.นี้ (ดูภายในเดือนนั้นๆ)  - ไม่ต้องเช็ค status  terminate
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits     
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header, natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type  
 		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-dividend_unpaid' as  group_nm
		, 'Accruedac_current'::varchar(100) as subgroup_nm --Outstanding Dividend (ส่วนเพิ่ม) without interest
		, 'Dividend ที่ไม่มีดอกเบี้ย ส่งเข้า V2 เฉพาะเดือนปัจจุบัน'  as subgroup_desc 
		, vn.variable_cd , vn.variable_name
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru   
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd)
		-- 04Aug2022 NarumolW. ChgNo.38  : Change condition to Lookup accode interest 
		--inner join stag_a.stga_ifrs_dividend_unpaid_summary dvmast  
		--on ( accru.policy_no = dvmast.policy_no   and dvmast.is_interest  = 0 ) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V2'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg  )	
		left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd 
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
		-- and pol.is_active = 0)-- Log.102 ดูเฉพาะ ไม่มีดอกเบี้ย แต่ไม่ต้องกรอง Terminate 
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 and natrn.benefit_gmm_flg in ('2.1','2.2' )
		 --and accru.accode = '5031010030'
		 and accru.system_type_cd in ( '034' ) -- ,'029')   
		 and not exists ( select 1 from dds.vw_ifrs_accrual_chg   b 
		 					where b.accode = '5031020030'  
		 					and accru.policy_no  = b.policy_no
		 					and accru.system_type_cd = b.system_type_cd)  -- ChgNo.38
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
						where bb.nac_rk = accru.accrual_rk 
						and source_filename = 'accrued-ac'); 
					
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP28 V2 DIVIDEND ACCRUAL- without INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP28 % : % row(s) - V2 DIVIDEND ACCRUAL-  without INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar; 

 	-- =========================================================================================================================== --
 	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits      
		select  natrn.ref_1 as ref_1, accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header , natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt
		--, paycmast.pcunpaid_amt as nac_amt 
 		--,case when  accru.dc_flg= acc.dc_flg then paycmast.pcunpaid_amt*inflow_flg else paycmast.pcunpaid_amt*inflow_flg*-1 end  as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm
		,'Accruedac_current'::varchar(100) as subgroup_nm --ACCRUAL-OTHERS
		, 'บันทึกค้างจ่ายเงินผลประโยชน์ ณ สิ้นเดือน'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , accru.ref_no as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V2'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)	 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd 
		 and accru.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 and natrn.benefit_gmm_flg not in ( '8' ,'2.1','2.2','18')  --  Benefit Log. 412
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
							where bb.nac_rk = accru.accrual_rk 
							and bb.source_filename = 'accrued-ac'); 

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP29 V2 COUPON ACCRUAL-NO INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP29 % : % row(s) - V2 Accrued Other',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 
	-- ===================================  V1 บันทึกจ่ายเงินผลประโยชน์ ======================================================== --
 	BEGIN
		--drop table if exists stag_a.stga_ifrs_nac_txn_step05_benefits;
		--create table stag_a.stga_ifrs_nac_txn_step05_benefits tablespace tbs_stag_a as 
		--truncate table stag_a.stga_ifrs_nac_txn_step05_benefits ; 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt ,nac.posting_amount as posting_amt  
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Pay Benefits'::varchar(100) as subgroup_nm , 'บันทึกจ่ายเงินผลประโยชน์' ::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from  stag_a.stga_ifrs_nac_txn_step04  natrn 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
 		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V1'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg
		 )		 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
		 					where bb.nac_rk = nac.nac_rk and bb.group_nm in ('natrn-nac','natrn-nac-dividend_paid','natrn-nac-coupon_paid')) 
		 and natrn.benefit_gmm_flg not in ( '2.2','8')
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP30 V1 Pay Benefits : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;  
	raise notice 'STEP30 % : % row(s) - V1 Pay Benefits',clock_timestamp()::varchar(19),v_affected_rows::varchar;
   

 	-- ===================================  V10 Policy Loan Recognition of Auto APL due to overdue premium =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no , accru.accode,accru.dc_flg , accru.ac_type 
		, accru.accru_amt
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd   
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		,accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Policy Loan' as subgroup_nm , 'Recognition of Auto APL due to overdue premium'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd) 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V10'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg
		 )		
		 where  accru.dc_flg = 'D'
		 and accru.doc_dt between v_control_start_dt and v_control_end_dt 
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = accru.accrual_rk and source_filename = 'accrued-ac'); 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP31 V10 Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP31 % : % row(s) - V10 Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- ===================================  V13 Policy Loan Clear APL สำหรับรายการที่เกิดระหว่างเดือน แต่ลงทุกสิ้นเดือน ( จาก V11 เดิม ) =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no , accru.accode,accru.dc_flg , accru.ac_type 
		, accru.accru_amt
		, accru.posting_accru_amount  as posting_amt --log.164 ยกเลิกกลับ Sign
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd   
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Policy Loan' as subgroup_nm , 'Clear APL สำหรับรายการที่เกิดระหว่างเดือน แต่ลงทุกสิ้นเดือน'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd) 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V13'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg
		 )		
		 where accru.dc_flg = 'C'
		 and accru.doc_dt between v_control_start_dt and v_control_end_dt  
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = accru.accrual_rk and source_filename = 'accrued-ac'); 
	 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP32 V13 Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP32 % : % row(s) - V13 Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- ===================================  V12 Policy Loan Recognition of policy loan - TLI lends customer loan =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Policy Loan'::varchar(100) as subgroup_nm 
		, 'Recognition of policy loan - TLI lends customer loan' ::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V12'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and natrn.post_dt  between pol.valid_fr_dttm and pol.valid_to_dttm )
		where  natrn.dc_flg = 'D'
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP33 V12 Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP33 % : % row(s) - V12 Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 09Aug2023 Narumol W. : [ITRQ#66083728] ขอเพิ่ม source payment_reject ใน V12 V13 V14 ของ Benefit_YTD
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk  
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel, ''::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject policy loan-DR' as subgroup_nm ,'รับรู้รายการเงินให้กู้ยืมกรมธรรม์ที่เกิดจากการให้ลูกค้ากู้ยืมเงิน (บันทึก เงินให้กู้ยืมกรมธรรม์ จากการให้ลูกค้ากู้ยืม)'::varchar(100)  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V12'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.dc_flg = 'D'
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP33 V12 Reject-Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP33 % : % row(s) - V12 Reject-Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
	begin
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk  
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel, ''::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse policy loan-DR' as subgroup_nm 
		, 'PAYMENT API Reverse รับรู้รายการเงินให้กู้ยืมกรมธรรม์ที่เกิดจากการให้ลูกค้ากู้ยืมเงิน (บันทึก เงินให้กู้ยืมกรมธรรม์ จากการให้ลูกค้ากู้ยืม)'::varchar(100)  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd  
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V12'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.dc_flg = 'D'
	    and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP33 V12 api_reverse-Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP33 % : % row(s) - V12 api_reverse-Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

 	-- ===================================  V13 Policy Loan Repayment policy loan =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt, posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Policy Loan'::varchar(100) as subgroup_nm 
		, 'Repayment policy loan' ::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V13'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and natrn.post_dt  between pol.valid_fr_dttm and pol.valid_to_dttm )
		where natrn.dc_flg = 'C'
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP34 V13 Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP34 % : % row(s) - V13 Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 09Aug2023 Narumol W. : [ITRQ#66083728] ขอเพิ่ม source payment_reject ใน V12 V13 V14 ของ Benefit_YTD
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk  
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel, ''::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject policy loan-CR' as subgroup_nm ,'ล้างรายการเงินให้กู้ยืมกรมธรรม์ทุก Event'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V13'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.dc_flg = 'C'
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP34 V13 Reject-Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP34 % : % row(s) - V13 Reject-Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg

	begin
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk  
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel, ''::varchar as pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse policy loan-CR' as subgroup_nm ,'PAYMENT API Reverse ล้างรายการเงินให้กู้ยืมกรมธรรม์ทุก Event'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd  
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V13'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg) 
		where  natrn.dc_flg = 'C'
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP34 V13 api_reverse-Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP34 % : % row(s) - V13 api_reverse-Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- ===================================  V14 Policy Loan Interst income from policy loan =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Interest on policy loan'::varchar(100) as subgroup_nm 
		, 'Interst income from policy loan' ::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V14'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and natrn.post_dt  between pol.valid_fr_dttm and pol.valid_to_dttm )
		where natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP35 V14 Interest on policy loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP35 % : % row(s) - V14 Interest on policy loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- ===================================  V14 Policy Loan Interest income from policy loan =========================== -- 
	BEGIN
 		insert into stag_a.stga_ifrs_nac_txn_step05_benefits      
		select  natrn.ref_1 as ref_1, accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header , natrn.post_dt 
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.accru_amt  as nac_amt , accru.posting_accru_amount  as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd  
		,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, accru.pay_period
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac-coupon_unpaid' as  group_nm,'COUPON ACCRUAL-NO INTEREST'::varchar(100) as subgroup_nm , 'บันทึกค้างจ่ายเงินผลประโยชน์ ณ สิ้นเดือน-แบบไม่ฝาก (อยู่ในระยะเวลารอคอย 7 วัน)'  as subgroup_desc 
		, vn.variable_cd ,vn.variable_name  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm    
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt
		 ) 
		left join  stag_s.ifrs_common_accode acc 
		on ( accru.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V14'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		)	 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where accru.doc_dt  between v_control_start_dt and v_control_end_dt
		 and natrn.detail not like 'ตั้งดอกเบี้ย%'
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = accru.accrual_rk and bb.source_filename = 'accrued-ac'); 

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP36 V14 COUPON ACCRUAL-NO INTEREST : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP36 % : % row(s) - V14 COUPON ACCRUAL-NO INTEREST',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	-- 09Aug2023 Narumol W. : [ITRQ#66083728] ขอเพิ่ม source payment_reject ใน V12 V13 V14 ของ Benefit_YTD
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk  
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.reject_amt 
		, reject.reject_posting_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel,''::varchar as pay_period 
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		, reject.natrn_dim_txt as refund_dim_txt  
		, 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  
		, 'Reject policy loan-CR' as subgroup_nm ,'รับรู้รายการดอกเบี้ยจากเงินให้กู้ยืม'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,reject.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
		on ( natrn.branch_cd =  reject.branch_cd 
		and natrn.doc_dt = reject.reject_doc_dt 
		and natrn.doc_type =  reject.reject_doc_type 
		and natrn.doc_no = reject.reject_doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V14'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)  
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP36 V14 Reject-Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP36 % : % row(s) - V14 Reject-Interest on Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg 

	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk,  natrn.natrn_x_dim_rk  
		, reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
		, natrn.accode , natrn.dc_flg , null::varchar as actype
		, reject.payment_api_reverse_amt 
		, reject.posting_payment_api_reverse_amt as posting_amt
		, reject.system_type ,reject.transaction_type,null::varchar as premium_type 
		, reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
		, null::date as pay_dt ,''::varchar as pay_by_channel,''::varchar as pay_period 
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
		, reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term ,reject.cost_center 
		, reject.reverse_dim_txt as refund_dim_txt  
		, 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
		, 'paymentapi_reverse policy loan-CR' as subgroup_nm ,'PAYMENT API Reverse รับรู้รายการดอกเบี้ยจากเงินให้กู้ยืม'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_destail 
		, reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		, reject.for_branch_cd 
		, natrn.event_type 
		, natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join  dds.ifrs_payment_api_reverse_chg reject  
		on ( natrn.branch_cd  =  reject.branch_cd 
		and natrn.doc_dt = reject.doc_dt 
		and natrn.doc_type =  reject.doc_type 
		and natrn.doc_no = reject.doc_no 
		and natrn.dc_flg = reject.dc_flg
		and natrn.accode = reject.accode 
		and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( reject.plan_cd = pp.plan_cd)  
		left outer join dds.tl_acc_policy_chg pol  
		on ( reject.policy_no = pol.policy_no 
		and reject.plan_cd = pol.plan_cd
		and  natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Benefit'
		and vn.variable_cd = 'V14'  
		and natrn.benefit_gmm_flg = vn.benefit_gmm_flg)    
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP36 V14 api_reverse-Policy Loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP36 % : % row(s) - V14 api_reverse-Interest on Policy Loan',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 --============================ V13 - Policy loan Payment Transpos (เงินให้กู้ยืมโดยมีกรมธรรม์เป็นประกัน) ======================================--
  -- Log.237
 	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
 		select  natrn.ref_1 as ref_1 , nac.natrn_rk  , nac.transpos_x_dim_rk  
        ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.ac_type 
        ,nac.refund_amt  as nac_amt 
        ,case when  nac.dc_flg= acc.dc_flg then nac.refund_amt*inflow_flg else nac.refund_amt*inflow_flg*-1 end  as posting_amt
        ,nac.system_type,nac.transaction_type,nac.premium_type
        ,nac.policy_no,nac.plan_cd, nac.rider_cd   
        ,null::date as pay_dt ,null::varchar as pay_by_channel
        ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
        ,nac.transpos_dim_txt  
        ,'payment_transpos' as source_filename  , 'natrn-payment_transpos'::varchar(100) as  group_nm , 'Policy Loan'::varchar(100) as subgroup_nm 
        ,'ล้างรายการเงินให้กู้ยืมกรมธรรม์'::varchar(100) as subgroup_desc  
        , vn.variable_cd , vn.variable_name as variable_nm 
        , natrn.detail as nadet_detail 
        , nac.org_sbranch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
        , case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type  
        ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
        ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
        from  stag_a.stga_ifrs_nac_txn_step04  natrn  
        inner join  dds.ifrs_payment_transpos_chg  nac  
        on ( natrn.org_branch_cd = nac.ref_branch_cd -- Log.381
        -- natrn.branch_cd = nac.branch_cd
        and natrn.doc_dt = nac.doc_dt 
        and natrn.doc_type = nac.doc_type
        and natrn.doc_no = nac.doc_no 
        and natrn.dc_flg = nac.dc_flg
        and natrn.accode = nac.accode
        and natrn.natrn_dim_txt = nac.transpos_dim_txt 
        )   
        left join  stag_s.ifrs_common_accode acc 
        on ( natrn.accode = acc.account_cd  )
        left join stag_s.stg_tb_core_planspec pp   
        on ( nac.plan_cd = pp.plan_cd)
        inner join stag_s.ifrs_common_variable_nm vn
        on ( vn.event_type = 'Benefit'
        and natrn.event_type = vn.event_type 
        and vn.variable_cd = 'V13'
        and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
        )        
        left outer join dds.tl_acc_policy_chg pol  
        on ( nac.policy_no = pol.policy_no
        and nac.plan_cd = pol.plan_cd
        and natrn.post_dt  between pol.valid_fr_dttm and pol.valid_to_dttm )
        where natrn.dc_flg = 'C'
        and natrn.post_dt  between v_control_start_dt and v_control_end_dt;  
               
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP36 V13 Policy loan Payment Transpos : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP36 % : % row(s) - V13 Policy Loan Payment Transpos',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
    --====================================
 	--Log.237 
 	begin
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
 			select  natrn.ref_1 as ref_1 , nac.natrn_rk , nac.transpos_x_dim_rk  
            ,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
            ,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,''::varchar as actype 
            ,nac.refund_amt as nac_amt 
            ,nac.posting_refund_amt  as posting_amt
            ,nac.system_type as system_type,''::varchar as transaction_type,''::varchar as premium_type
            ,nac.policy_no,coalesce(pol.plan_cd,nac.plan_cd) as plan_cd ,nac.rider_cd 
            ,null::date as pay_dt ,''::varchar as pay_by_channel
            ,null::varchar as pay_period
            ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
            ,natrn.sales_id,natrn.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
            ,nac.transpos_dim_txt  
            ,'payment_transpos' as source_filename  , 'natrn-payment_transpos'::varchar(100) as  group_nm , 'Interest on Policy Loan'::varchar(100) as subgroup_nm 
            , 'รับรู้รายการดอกเบี้ยจากเงินให้กู้ยืม'::varchar(100) as subgroup_desc  
            , vn.variable_cd , vn.variable_name as variable_nm 
            , natrn.detail as nadet_detail 
            , nac.branch_cd::varchar(10) as org_branch_cd
            , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
            ,natrn.for_branch_cd
            ,natrn.event_type  
            ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
            ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
            ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
            ,pp.ifrs17_portid ,pp.ifrs17_portgroup 
            ,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
            from  stag_a.stga_ifrs_nac_txn_step04  natrn  
            inner join  dds.ifrs_payment_transpos_chg nac 
            on (  natrn.org_branch_cd = nac.ref_branch_cd -- Log.381
        	-- natrn.branch_cd = nac.branch_cd
            and natrn.doc_dt = nac.doc_dt 
            and natrn.doc_type = nac.doc_type
            and natrn.doc_no = nac.doc_no 
            and natrn.dc_flg = nac.dc_flg
            and natrn.accode = nac.accode
            and natrn.natrn_dim_txt = nac.transpos_dim_txt  )   
            left join  stag_s.ifrs_common_accode acc 
            on ( natrn.accode = acc.account_cd  )
            left outer join dds.tl_acc_policy_chg pol  
            on ( nac.policy_no = pol.policy_no
            and  nac.plan_cd = pol.plan_cd -- Oil 25jan2023 : add plan_cd
            and natrn.post_dt  between pol.valid_fr_dttm and pol.valid_to_dttm )                
            left join stag_s.stg_tb_core_planspec pp   
            on ( pol.plan_cd = pp.plan_cd)
            inner join stag_s.ifrs_common_variable_nm vn
            on ( vn.event_type = 'Benefit'
            and natrn.event_type = vn.event_type 
            and vn.variable_cd = 'V14'
            and vn.benefit_gmm_flg = natrn.benefit_gmm_flg  ) 
            where natrn.post_dt  between v_control_start_dt and v_control_end_dt;	
		   
           GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP36 V14 Interest on Policy Loan  : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP36 % : % row(s) - V14 Interest on Policy Loan ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 
  	-- ===================================  V14 Premium interest =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1 , nac.premium_interest_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,''::varchar as actype 
		,nac.cash_amt as nac_amt 
		,nac.interest_posting_amt  as posting_amt
		,''::varchar as system_type,''::varchar as transaction_type,''::varchar as premium_type
		,nac.policy_no,pol.plan_cd,null as rider_cd   
		,null::date as pay_dt ,''::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.natrn_dim_txt  
		,nac.filename as source_filename  , 'natrn-premium_interest'::varchar(100) as  group_nm , 'Premium interest from insert receipt module'::varchar(100) as subgroup_nm 
		, 'ข้อมูลรายการรับดอกเบี้ยเบี้ยประกันผ่าน module insert ใบเสร็จ'::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.branch_cd::varchar(10) as org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.ifrs_premium_interest_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.natrn_dim_txt )   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 --and nac.plan_cd = pol.plan_cd
		 -- [ITRQ#66083736] ขอเพิ่มเงื่อนไขการ mapping source nrcptyymm ใน V14 ของ Benefit_YTD ไม่รวม mortgate , group eb
		 and pol.policy_type not in ('M','G','B') 
		 and natrn.post_dt  between pol.valid_fr_dttm and pol.valid_to_dttm )		
		left join stag_s.stg_tb_core_planspec pp   
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd = 'V14'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg  )
		where natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP35 V14 Interest on policy loan : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP37 % : % row(s) - V14 Premium Interest',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 /*
 	-- ===================================  V1 =========================== -- 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Policy Loan - CLAIMPOS'::varchar(100) as subgroup_nm  
		, 'Repayment of policy loan,included Interst on PL, by offsetting with death claim' ::varchar(100) as subgroup_desc  
		, vn.variable_cd 
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,1 as is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd in ( 'V1') --Log.20
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		 left outer join stag_a.stga_tl_acc_policy pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd )
		 where coalesce(natrn.is_reverse,0) <> 1    
		 -- and system_type  = 'CLAIMPOS' 
		 
		 and natrn.branch_cd <> '000' --Log.69
		 and nac.filename <> 'nacpaylink'
		 and natrn.benefit_gmm_flg  = '8' -- 8	Coupon
		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = nac.nac_rk ) 
		 and not exists ( select 1 from stag_a.stga_ifrs_coupon_paid_summary cp where cp.policy_no = nac.policy_no and cp.is_deposit = 1 ) 
		 
		 select * from  stag_a.stga_ifrs_coupon_paid_summary where policy_no  = '37337673'
		 select * from  stag_a.stga_ifrs_coupon_paid where policy_no  = '37337673'

		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP30 V15 Policy Loan- CLAIMPOS : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP30 % : % row(s) - V15 Policy Loan- CLAIMPOS',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 */
 
 	-- 29Jun2022 - ยกเลิกV15 เปลี่ยนไปใช้ Ending Balance จาก Data Smile
 	-- Insert ไว้ก่อน เพื่อไม่ให้เข้าเงื่อนไข DUMMY แล้วค่อยไป DELETE ขั้นตอนสุดท้าย
 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits   
		select  natrn.ref_1 as ref_1 , nac.nac_rk , nac.natrn_x_dim_rk  
		,nac.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.amount as nac_amt 
		,case when  nac.dc_flg= acc.dc_flg then nac.amount*inflow_flg else nac.amount*inflow_flg*-1 end  as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd   
		,nac.pay_dt ,nac.pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'Policy Loan - CLAIMPOS'::varchar(100) as subgroup_nm  
		, 'Repayment of policy loan,included Interst on PL, by offsetting with death claim' ::varchar(100) as subgroup_desc  
		, vn.variable_cd 
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,1 as is_duplicate_variable , vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn  
		inner join  dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.dc_flg = nac.dc_flg
		and natrn.accode = nac.accode
		and natrn.natrn_dim_txt = nac.nac_dim_txt 
		and natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk  -- Log.247
		)   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		left join stag_s.stg_tb_core_planspec pp   
		on ( nac.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type 
		 and vn.variable_cd in ( 'V15') --Log.20 
		 )	 
 		 left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where system_type  = 'CLAIMPOS' 
		 and natrn.branch_cd <> '000' --Log.69
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP37 V15 Policy Loan- CLAIMPOS : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP37 % : % row(s) - V15 Policy Loan- CLAIMPOS',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- =================================== ADTH =========================== -- 

	BEGIN 	
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , adth.adth_rk  , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,adth.ref_doc_dt doc_dt,natrn.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.claim_amt 
		,adth.posting_claim_amt as posting_sum_nac_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd  ,null::date pay_dt ,null::varchar pay_by_channel
		,null::varchar as pay_period
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as nac_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'claim' as subgroup_nm ,  'claim' as subgroup_desc 
		,vn.variable_cd
		,vn.variable_name as variable_nm  
		,natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04  natrn
		 inner join dds.oic_adth_chg adth   
		 on (   adth.branch_cd = natrn.branch_cd  
			and adth.ref_doc_dt = natrn.doc_dt 
		  	--and adth.doc_type = natrn.doc_type
		 	and adth.ref_doc_no = natrn.doc_no 
		 	and adth.dc_flg = natrn.dc_flg 
		 	and adth.accode = natrn.accode 
			and adth.adth_dim_txt = natrn.natrn_dim_txt  	)
		 left join stag_s.ifrs_common_accode  acc 
		 on ( natrn.accode = acc.account_cd  )
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd) 
 		 left outer join dds.tl_acc_policy_chg pol  
		 on ( adth.policy_no = pol.policy_no
		 and adth.plan_cd = pol.plan_cd
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V1' 
		 and natrn.event_type  = vn.event_type
		 and natrn.benefit_gmm_flg  = vn.benefit_gmm_flg 
		  )	
		 where not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_benefits bb where adth.adth_rk = bb.nac_rk and bb.group_nm = 'natrn-claim') 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP38 V1 ADTH : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP38 % : % row(s) - V1 ADTH',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	--================ V1 - บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน (ไม่มีดอกเบี้ย) =============================--
	BEGIN	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 	
		select  natrn.ref_1 as ref_1 , adth.adth_fax_claim_rk as adth_rk , natrn.natrn_x_dim_rk   
                ,adth.branch_cd 
                ,natrn.sap_doc_type,natrn.reference_header::varchar(100) ,natrn.post_dt              
                ,adth.doc_dt,natrn.doc_type,adth.doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
                ,adth.claim_amt  as nac_amt ,adth.posting_claim_amt as posting_amt                
                ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
                ,adth.policy_no,adth.plan_cd, adth.rider_cd  
                ,null::date as  pay_dt ,null::varchar as pay_by_channel
                ,null::varchar as pay_period
                ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
                ,adth.sales_id,adth.sales_struct_n    as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
                ,adth.adthtran_dim_txt  as nac_dim_txt  
                ,adth.source_filename::varchar(100) , 'natrn-fax claim'::varchar(100) as  group_nm  
                , 'benefit'::varchar(100)  as subgroup_nm 
                ,  'บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน'::varchar(100)  as subgroup_desc 
                 , vn.variable_cd
                , vn.variable_name as variable_nm  
                , natrn.detail as nadet_detail 
                , adth.org_branch_cd,null::varchar as org_submit_no
                , null::varchar(20) as submit_no
                , adth.section_order_no
                , 0::int as is_section_order_no
                ,natrn.for_branch_cd
                ,natrn.event_type  
                ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
                ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
                ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
                ,pp.ifrs17_portid ,pp.ifrs17_portgroup
                ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm  
                from  stag_a.stga_ifrs_nac_txn_step04  natrn
                 inner join dds.ifrs_adth_payment_api_chg adth  
                 on ( adth.branch_cd = natrn.branch_cd  
                 and adth.doc_dt = natrn.doc_dt  
                 and adth.doc_no = natrn.doc_no 
                 and adth.accode = natrn.accode 
                 and adth.dc_flg = natrn.dc_flg   
                 and adth.adthtran_dim_txt  = natrn.natrn_dim_txt ) 
                 left join stag_s.ifrs_common_accode  acc 
                 on ( natrn.accode = acc.account_cd  )
                 left join  stag_s.stg_tb_core_planspec pp 
                 on ( adth.plan_cd = pp.plan_cd)
                 inner join stag_s.ifrs_common_variable_nm vn
                 on ( vn.event_type = 'Benefit'
                 and vn.variable_cd = 'V1'
                 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
                 )
                left outer join dds.tl_acc_policy_chg pol  
                 on (  adth.policy_no = pol.policy_no
                 and  adth.plan_cd = pol.plan_cd
                 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
                 where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = adth.adth_fax_claim_rk )
                 --and natrn.benefit_gmm_flg not in ('8','2.1','2.2') 
                 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
                 				where cc.accode = '5031020020'  and cc.group_nm = 'natrn-acctbranch_nacpaylink' and cc.policy_no  = adth.policy_no ) 
                 and natrn.post_dt between v_control_start_dt and v_control_end_dt   ;
                GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
				raise notice 'STEP38 % : % row(s) - V1 - บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน (ไม่มีดอกเบี้ย)',clock_timestamp()::varchar(19),v_affected_rows::varchar;
                
 		--================== V5 - บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน (มีดอกเบี้ย) =============================--	
           	insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
			select  natrn.ref_1 as ref_1 , adth.adth_fax_claim_rk as adth_rk , natrn.natrn_x_dim_rk   
            ,adth.branch_cd 
            ,natrn.sap_doc_type,natrn.reference_header::varchar(100) ,natrn.post_dt              
            ,adth.doc_dt,natrn.doc_type,adth.doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
            ,adth.claim_amt  as nac_amt ,adth.posting_claim_amt as posting_amt                
            ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
            ,adth.policy_no,adth.plan_cd, adth.rider_cd  
            ,null::date as  pay_dt ,null::varchar as pay_by_channel
            ,null::varchar as pay_period
            ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
            ,adth.sales_id,adth.sales_struct_n    as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
            ,adth.adthtran_dim_txt  as nac_dim_txt  
            ,adth.source_filename::varchar(100) , 'natrn-fax claim'::varchar(100) as  group_nm  
            ,'benefit'::varchar(100)  as subgroup_nm 
            ,'บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน'::varchar(100)  as subgroup_desc 
            , vn.variable_cd
            , vn.variable_name as variable_nm  
            , natrn.detail as nadet_detail 
            , adth.org_branch_cd,null::varchar as org_submit_no
            , null::varchar(20) as submit_no
            , adth.section_order_no
            , 0::int as is_section_order_no
            ,natrn.for_branch_cd
            ,natrn.event_type  
            ,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
            ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
            ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
            ,pp.ifrs17_portid ,pp.ifrs17_portgroup
            ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm
            from  stag_a.stga_ifrs_nac_txn_step04  natrn
            inner join dds.ifrs_adth_payment_api_chg adth  
            on ( adth.branch_cd = natrn.branch_cd  
            and adth.doc_dt = natrn.doc_dt  
            and adth.doc_no = natrn.doc_no 
            and adth.accode = natrn.accode 
            and adth.dc_flg = natrn.dc_flg   
            and adth.adthtran_dim_txt  = natrn.natrn_dim_txt ) 
            left join stag_s.ifrs_common_accode  acc 
            on ( natrn.accode = acc.account_cd  )
            left join  stag_s.stg_tb_core_planspec pp 
            on ( adth.plan_cd = pp.plan_cd)
            inner join stag_s.ifrs_common_variable_nm vn
            on ( vn.event_type = 'Benefit'
            and vn.variable_cd = 'V5'
            and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
             )
            left outer join dds.tl_acc_policy_chg pol  
            on (  adth.policy_no = pol.policy_no
            and  adth.plan_cd = pol.plan_cd
            and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
            where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.nac_rk = adth.adth_fax_claim_rk )
            and natrn.benefit_gmm_flg in ('8')
            and exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits cc 
            			where cc.accode = '5031020020'  and cc.group_nm = 'natrn-fax claim' and cc.policy_no  = adth.policy_no ) 
            and natrn.post_dt between v_control_start_dt and v_control_end_dt   ;
				
            GET DIAGNOSTICS v_affected_rows = ROW_COUNT;		
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP38 V1 ADTH : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
			RETURN out_err_cd ;  			
		END;  
	 	raise notice 'STEP38 % : % row(s) - V5 - บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน (มีดอกเบี้ย)',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

/*
		-- =================================== ADTH =========================== -- 

	BEGIN 	
		
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , adth.adth_rk  , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,adth.ref_doc_dt doc_dt,adth.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype 
		--,case when  adth.dc_flg= adth.dc_flg then adth.claim_amt*inflow_flg else adth.claim_amt*inflow_flg*-1 end  as posting_amt --
		,adth.claim_amt 
		,adth.posting_claim_amt as posting_sum_nac_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd  ,null::date pay_dt ,null::varchar pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as nac_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'claim' as subgroup_nm ,  'claim' as subgroup_desc 
		,vn.variable_cd
		,vn.variable_name as variable_nm  
		,natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04  natrn
		 inner join dds.oic_adth_chg adth   
		 on (   adth.branch_cd = natrn.branch_cd  
			and adth.ref_doc_dt = natrn.doc_dt 
		  	and adth.doc_type = natrn.doc_type
		 	and adth.ref_doc_no = natrn.doc_no 
		 	and adth.dc_flg = natrn.dc_flg 
			and adth.adth_dim_txt = natrn.natrn_dim_txt  	)
		 left join stag_s.ifrs_common_accode  acc 
		 on ( natrn.accode = acc.account_cd  )
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 left outer join stag_a.stga_tl_acc_policy pol  
		 on ( adth.policy_no = pol.policy_no
		 and adth.plan_cd = pol.plan_cd )
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Benefit'
		 and vn.variable_cd = 'V14' 
		 and natrn.event_type  = vn.event_type
		 and natrn.benefit_gmm_flg  = vn.benefit_gmm_flg 
		  )	
		 where coalesce(natrn.is_reverse,0) <> 1  
  		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_benefits bb where adth.adth_rk = bb.nac_rk and bb.group_nm = 'natrn-claim') 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP29 V14 ADTH : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  			
	END;  
	raise notice 'STEP30 % : % row(s) - V14 Policy Loan - ADTH',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	*/
-- =================================== MANUAL =========================== -- 
	BEGIN	
		/*
		drop table if exists stag_a.stga_ifrs_nac_txn_step01_2;
		create table stag_a.stga_ifrs_nac_txn_step01_2 tablespace tbs_stag_a
		as 
		select company_code, fiscal_year,doc_no  ,ref1_header,doc_type ,posting_date_dt
			,account_no,sap_dim_txt
			,sum(posting_sap_amt) as  posting_sap_amt
			from stag_a.stga_ifrs_nac_txn_step01  
			where posting_date_dt  between v_control_start_dt and v_control_end_dt 
			group by  company_code, fiscal_year,doc_no  ,ref1_header,doc_type ,posting_date_dt
			,account_no,sap_dim_txt; 
		*/
		------------- Manual  
		-- length(natrn.nadet_policyno) <= 8  STEP_01
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.natrn_amt as nac_amt 
		,natrn.posting_natrn_amt as posting_amt
		 ,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd)
		,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename  
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd) 
 		inner join dds.tl_acc_policy_chg pol  
		on ( natrn.nadet_policyno = pol.policy_no 
		and natrn.plan_cd = pol.plan_cd
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type  
		 and vn.variable_cd  = 'V1'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Benefit'
		and natrn.accode not in ( '2013010020','2995020120' )
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and natrn.doc_dt  between v_control_start_dt and v_control_end_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP39.1 % : % row(s) - VX - Manual DOCNO 5  STEP_01 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.natrn_amt as nac_amt 
		,natrn.posting_natrn_amt as posting_amt
		 ,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd)
		,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename  
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd) 
 		left outer join dds.tl_acc_policy_chg pol  
		on ( natrn.nadet_policyno = pol.policy_no 
		-- 13Nov2023 Narumol W. : Benefit Log 403
		and pol.policy_type not in ('M','G','B')
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm) 
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type  
		 and vn.variable_cd  = 'V1'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Benefit'
		and natrn.accode not in ( '2013010020','2995020120' )
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and natrn.doc_dt  between v_control_start_dt and v_control_end_dt
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits c 
  		 					where natrn.branch_cd = c.branch_cd
  		 					and natrn.doc_dt = c.doc_dt  
  		 					and natrn.doc_type = c.doc_type  
  		 					and natrn.doc_no = c.doc_no  
  		 					and natrn.accode = c.accode
  		 					and natrn.nadet_policyno = c.policy_no
  		 					); 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP39.2 % : % row(s) - VX - Manual DOCNO 5  STEP_01 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_benefits' 				
				,'ERROR STEP39 VX Manual DOCNO 5: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;		
	
	BEGIN	
        -- length(natrn.nadet_policyno) > 9   STEP_02
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.natrn_amt as nac_amt 
		,natrn.posting_natrn_amt as posting_amt
		 ,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd)
		,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,null::varchar as pay_period
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename  
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.benefit_gmm_flg,natrn.benefit_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd) 
 		left outer join dds.tl_acc_policy_chg pol  
		on ( trim(natrn.nadet_policyno) = concat(trim(pol.plan_cd),trim(pol.policy_no) )
		and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm 
		and pol.policy_type in ('M','G','B'))
		inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Benefit'
		 and natrn.event_type = vn.event_type  
		 and vn.variable_cd  = 'V1'
		 and vn.benefit_gmm_flg = natrn.benefit_gmm_flg 
		 )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Benefit'
		and natrn.accode not in ( '2013010020','2995020120' )
		and length(nullif(trim(natrn.nadet_policyno),''))> 9 
		and natrn.doc_dt  between v_control_start_dt and v_control_end_dt ; 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP39 VX Manual DOCNO 5: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP39 % : % row(s) - VX - Manual DOCNO 5  STEP_02',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

-- =================================== MANUAL from GL SAP =========================== -- 
	
		-- Oil 14Nov2022 : เพิ่ม policy_no , plan_cd
		-- Oil 13Jan2023 : Log 269 add filter dc_flg and union 

		-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
	 	/* ปรับเป็น การแบ่งกลุ่มข้อมูลดังนี้ 
		40.1 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขเดิม : V1,V4,V5,V14,V7,V12 and benefit_gmm_flg  not in ( '8','2.1','2.2') 
		40.2 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขใหม่
			- V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
			- V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
			- V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
			- V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
		*/
	begin
		-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
		-- [ITRQ#67010228] 40.1 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขเดิม : V1,V4,V5,V14,V7,V12 and benefit_gmm_flg  not in ( '8','2.1','2.2') 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode  , dc_flg 
		, nac_amt  
		, posting_amt  
		, policy_no 
		, plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,benefit_gmm_flg,benefit_gmm_desc
		, policy_type, effective_dt,issued_dt
		, ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable
		, rider_cd)		
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd) 
		from stag_a.stga_ifrs_nac_txn_step01 gl 
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  -- ไม่มี policy_no ทำให้ไม่ได้ ref_1 จาก gl จึงลง dummy 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)  
	    -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Change to inner join
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit' 
	    -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
	    and ( vn.variable_cd in ('V1','V4','V14','V7','V12') -- เพิ่ม V12 เอา V10 ออก Log 282
			or coa.benefit_gmm_flg in ('18','19')) -- Benefit Log 406
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg )    
	    where  not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  ) -- Log.228 1116120700
	    and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120') 
		-- [ITRQ#67010228] 40.1 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขเดิม : V1,V4,V5,V14,V7,V12 and benefit_gmm_flg  not in ( '8','2.1','2.2') 
		and coa.benefit_gmm_flg not in ( '8','2.1','2.2') 
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP40.1 % : % row(s) - VX - Manual SAP not in Coupon,Dividend',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP40.1 VX - Manual SAP not in Coupon,Dividend : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  

	begin
		-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
		-- [ITRQ#67010228] 40.2 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขใหม่
		/*	- V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
			- V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
			- V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
			- V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
		*/
		-- [ITRQ#67010228] V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode  , dc_flg 
		, nac_amt  
		, posting_amt  
		, policy_no 
		, plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,benefit_gmm_flg,benefit_gmm_desc
		, policy_type, effective_dt,issued_dt
		, ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable 
		, rider_cd)		
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
		from stag_a.stga_ifrs_nac_txn_step01 gl 
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  -- ไม่มี policy_no ทำให้ไม่ได้ ref_1 จาก gl จึงลง dummy 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)   
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit'  
	    and vn.variable_cd ='V1'
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg 
		-- [ITRQ#67010228] V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
	    and coa.benefit_gmm_flg = '8')  
	    where not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
	    				where bb.policy_no  = coalesce(gl.policy_no,pol.policy_no)
	    				and bb.plan_cd  = coalesce(gl.plan_cd,pol.plan_cd)
                        and bb.accode   = '5031020020' )   
	    and not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  )  
        and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120')  
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP40.2 % : % row(s) - V1 : benefit_gmm_flg =8 ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = 5031020020'
		,clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP40.2 - V1 : benefit_gmm_flg =8 ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = 5031020020 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 

	begin
		-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
		-- [ITRQ#67010228] 40.2 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขใหม่
		/*	- V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
			- V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
			- V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
			- V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
		*/
		-- [ITRQ#67010228] V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode  , dc_flg 
		, nac_amt  
		, posting_amt  
		, policy_no 
		, plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,benefit_gmm_flg,benefit_gmm_desc
		, policy_type, effective_dt,issued_dt
		, ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable
		, rider_cd)		
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd) 
		from stag_a.stga_ifrs_nac_txn_step01 gl 
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  -- ไม่มี policy_no ทำให้ไม่ได้ ref_1 จาก gl จึงลง dummy 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)   
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit'  
	    and vn.variable_cd ='V5'
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg 
		-- [ITRQ#67010228] V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
	    and coa.benefit_gmm_flg = '8')  
	    where exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
	    				where bb.policy_no  = coalesce(gl.policy_no,pol.policy_no)
	    				and bb.plan_cd  = coalesce(gl.plan_cd,pol.plan_cd)
                        and bb.accode   = '5031020020' )   
	    and not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  )  
        and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120')  
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP40.3 % : % row(s) - V1 : benefit_gmm_flg =8 มีการบันทึกบัญชีดอกเบี้ย exists accode = 5031020020'
		,clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP40.3 - V1 : benefit_gmm_flg =8 มีการบันทึกบัญชีดอกเบี้ย exists accode = 5031020020 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  

	begin
		-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
		-- [ITRQ#67010228] 40.2 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขใหม่
		/*	- V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
			- V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
			- V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
			- V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
		*/
		-- [ITRQ#67010228] - V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode  , dc_flg 
		, nac_amt  
		, posting_amt  
		, policy_no 
		, plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,benefit_gmm_flg,benefit_gmm_desc
		, policy_type, effective_dt,issued_dt
		,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable
		, rider_cd)		
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd) 
		from stag_a.stga_ifrs_nac_txn_step01 gl 
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  -- ไม่มี policy_no ทำให้ไม่ได้ ref_1 จาก gl จึงลง dummy 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)   
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit'  
	    and vn.variable_cd ='V1'
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg 
		-- [ITRQ#67010228] - V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
	    and coa.benefit_gmm_flg in ('2.1','2.2' ) )  
	    where not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
	    				where bb.policy_no  = coalesce(gl.policy_no,pol.policy_no)
	    				and bb.plan_cd  = coalesce(gl.plan_cd,pol.plan_cd)
                        and bb.accode   = '5031020030' )   
	    and not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  )  
        and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120')  
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP40.4 % : % row(s) - V1 :  benefit_gmm_flg in (2.1,2.2) ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = 5031020030'
		,clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP40.4 - V1 : benefit_gmm_flg in (2.1,2.2) ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = 5031020030 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;   
	END; 
	begin
		-- 29Jan2024 Narumol W. : [ITRQ#67010228] Enhance Manual SAP condition of Coupon Dividend in BENEFIT_YTD
		-- [ITRQ#67010228] 40.2 Variable ที่ไม่กระทบกับ Coupon , Dividend ใช้เงื่อนไขใหม่
		/*	- V1 : benefit_gmm_flg = '8' ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020020' 
			- V5 : benefit_gmm_flg = '8' มีการบันทึกบัญชีดอกเบี้ย exists accode = '5031020020' 
			- V1 : benefit_gmm_flg in ('2.1','2.2') ไม่มีการบันทึกบัญชีดอกเบี้ย not exists accode = '5031020030' 
			- V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
		*/
		-- [ITRQ#67010228] - V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode  , dc_flg 
		, nac_amt  
		, posting_amt  
		, policy_no 
		, plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,benefit_gmm_flg,benefit_gmm_desc
		, policy_type, effective_dt,issued_dt
		,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable 
		, rider_cd)		
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
		from stag_a.stga_ifrs_nac_txn_step01 gl 
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  -- ไม่มี policy_no ทำให้ไม่ได้ ref_1 จาก gl จึงลง dummy 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)   
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit'  
	    and vn.variable_cd ='V6'
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg 
		-- [ITRQ#67010228] - V6 : benefit_gmm_flg in ('2.1','2.2') มีการบันทึกบัญชีดอกเบี้ย exists accode =  '5031020030'  
	    and coa.benefit_gmm_flg in ('2.1','2.2' ) )  
	    where exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb 
	    				where bb.policy_no  = coalesce(gl.policy_no,pol.policy_no)
	    				and bb.plan_cd  = coalesce(gl.plan_cd,pol.plan_cd)
                        and bb.accode   = '5031020030' )   
	    and not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  )  
        and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120')  
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP40.5 % : % row(s) - V6 :  benefit_gmm_flg in (2.1,2.2) มีการบันทึกบัญชีดอกเบี้ย exists accode = 5031020030'
		,clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP40.5 - V6 : benefit_gmm_flg in (2.1,2.2) มีการบันทึกบัญชีดอกเบี้ย exists accode = 5031020030 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;   
	END; 
		
		
		/* Comment to use [ITRQ#67010228] instead 
	BEGIN
		------------- Manual    
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode  , dc_flg 
		, nac_amt  
		, posting_amt  
		, policy_no 
		, plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,benefit_gmm_flg,benefit_gmm_desc
		, policy_type, effective_dt,issued_dt
		,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable   )		
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		from stag_a.stga_ifrs_nac_txn_step01 gl
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  -- ไม่มี policy_no ทำให้ไม่ได้ ref_1 จาก gl จึงลง dummy 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)  
	    -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Change to inner join
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit' 
	    -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
	    and ( vn.variable_cd in ('V1','V4','V14','V7','V12') -- เพิ่ม V12 เอา V10 ออก Log 282
			or coa.benefit_gmm_flg = '18') -- Benefit Log 406
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg )    
	    where  not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  ) -- Log.228 1116120700
	    and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120')
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt
		and dc_flg ='D' 
		union all 
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  
		, local_sap_amt 
		, posting_sap_amt 
		, coalesce(gl.policy_no,pol.policy_no) as policy_no
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		, 'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.benefit_gmm_flg , coa.benefit_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		from stag_a.stga_ifrs_nac_txn_step01 gl
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Benefit')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd = pol.plan_cd  -- Log 230
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm )   
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)  
	    -- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Change to inner join
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Benefit' 
	   	-- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : Add condition for V5-'Interest of Deposit'
	    and ( vn.variable_cd in ('V1','V4','V13','V14','V7') or coa.benefit_gmm_flg = '18') -- Benefit Log 406
	    and coa.benefit_gmm_flg   = vn.benefit_gmm_flg )    
	    where  not exists  ( select 1 from stag_a.stga_ifrs_nac_txn_step05_benefits bb where bb.ref_1 = gl.ref1_header 
                        and bb.accode   = gl.account_no  
                        and bb.nac_dim_txt  = gl.sap_dim_txt  ) -- Log.228 1116120700
	    and doc_type not in ('SI','SB')
	 	and gl.account_no not in  ( '2013010020','2995020120')
		and gl.posting_date_dt between v_control_start_dt and v_control_end_dt
		and dc_flg ='C' 	;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_premium' 				
				,'ERROR STEP40 VX Manual PROXY : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP40 % : % row(s) - VX - Manual PROXY ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	*/
 
	-- =================================== RECONCILE ======================================================== -- 
 
	-- Reconcile 		
 	BEGIN	
 
		-- Reconcile -- select * from stag_a.stga_ifrs_nac_txn_step04 where natrn_x_dim_rk is null 
		--drop table if exists stag_a.stga_ifrs_nac_txn_missing_step05_benefits;
		--create table stag_a.stga_ifrs_nac_txn_missing_step05_benefits tablespace tbs_stag_a as 
		
		truncate table stag_a.stga_ifrs_nac_txn_missing_step05_benefits;
	
		insert into stag_a.stga_ifrs_nac_txn_missing_step05_benefits
		select distinct *
		from  (   
		-- Cover V1,V2,V3,V4,V5,V6,V7
		select step4.*  ,null::varchar as variable_cd ,1::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on ( step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1  is null  -- and step4.natrn_x_dim_rk is null  
		and step4.benefit_gmm_flg not in ( '14','15','16','13','17') -- Manual input GL_ACCOUNT_BALANCE_SEGMENT
		and step4.benefit_gmm_flg not in ( '5','10.1') -- Cover V1 filter system_type_cd = 037 , เอา 12.2 ออก Log.251 
	    and step4.benefit_gmm_flg in ('2.1','2.2','8') -- Cover V1,V2,V3,V4,V5,V6
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt   
		
	
		/* --Log 329 Track_chg.81
		-- Cover V14
		union all -- Log no.63 
		select step4.* ,null::varchar as variable_cd ,2::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on (   step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  
		and step4.ref_1 like 'AC%'
		and step4.detail not like 'ตั้งดอกเบี้ย%'
		and step4.benefit_gmm_flg in ('17')
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt  
		*/
		-- Cover V14
		union all -- Log no.251 
		select step4.* ,null::varchar as variable_cd ,2::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on (   step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  
		and step4.ref_1 not like 'AC%'
		and step4.benefit_gmm_flg in ('17')
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt  
		
		-- Cover V14
		union all -- Log no.26 
		select step4.* ,null::varchar as variable_cd ,2::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on (   step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  
		and step4.ref_1 like 'BRN%' 
		and step4.accode = '4189010010' -- & not exists in nac 
		--and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt   	
		
		-- Cover V14,V7
		union all -- Log no.26 
		select step4.* ,null::varchar as variable_cd ,2::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on (   step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no
		and step4.accode = step5.accode  
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  
		and step4.ref_1 in ('CLM','MAT') --Log.112
		and  step4.benefit_gmm_flg ='17' -- & not exists in nac 
		--and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt   	
		
		--Cover V10,V12,V13
		union all  
		select step4.*    
		,case when step4.ref_1  like 'AC%' and  step4.dc_flg ='D' then 'V10'
			when step4.ref_1 not like 'AC%' and step4.dc_flg ='D' then 'V12'
			when step4.dc_flg ='C' then 'V13' end as variable_cd  -- V13 include V11
			--when step4.ref_1  like 'AC%' and  step4.dc_flg ='C' then 'V13'
			--when step4.ref_1 not like 'AC%' and step4.dc_flg ='C' then 'V13' end as variable_cd  
		,3::int as priority_cd 
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on ( step4.ref_1 = step5.ref_1
		and step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  
		and step4.benefit_gmm_flg = '16' 
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt 
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')		
				
		-- Cover OTHERS
		union all 
		
		select step4.* ,null::varchar as variable_cd ,4::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on ( step4.ref_1 = step5.ref_1
		and step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  
		and step4.benefit_gmm_flg not in ( '14','15','16','17') -- Manual input GL_ACCOUNT_BALANCE_SEGMENT  , เเอา 13 ออก Log.251
		and step4.benefit_gmm_flg not in ( '5','10.1','12.2') -- Cover V1 filter system_type_cd = 037
		-- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : add benefit_gmm_flg = 18
		and step4.benefit_gmm_flg not in ( '1','2.1','2.2','8','18','19')  -- Cover V1,V2,V3,V4,V5,V6,V7
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt  
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		 
		-- Cover V1 Log.251
		union all 
		select step4.*  ,null::varchar as variable_cd ,1::int as priority_cd
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on ( step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no 
		and step4.accode = step5.accode -- 16Dec2022 Narumol W. : add key accode 
		and step4.natrn_dim_txt = step5.nac_dim_txt) -- 05Jan2023 Oil : add key natrn_dim_txt Log.272 
		where step5.ref_1 is null  -- and step4.natrn_x_dim_rk is null  
		-- 29Nov2023 Narumol W. : [ITRQ#66115242] ปรับปรุงเงื่อนไข Coupon Interest ใน BENEFIT_YTD : add benefit_gmm_flg = 18
		and step4.benefit_gmm_flg in ( '5','10.1','12.2','1','18','19') -- Cover V1 filter system_type_cd = 037  ,เพิ่ม 12.2 เข้ามา Log.251 ,benefit_gmm_flg = 1 Log.281
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit')
		and step4.org_branch_cd <>'A00' -- ไม่เอา org_branch_cd ที่เป็น accru
		and step4.doc_dt between  v_control_start_dt and v_control_end_dt   
		
		) as aa ;
	 
	/*
		select step4.* --  step4.ref_1 , step4.branch_cd ,step4.doc_dt , step4.doc_type,step4.doc_no, step4.accode ,sum(posting_natrn_amt) as posting_natrn_amt
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_benefits step5
		on ( step4.natrn_x_dim_rk = step5.natrn_x_dim_rk
		and  step4.ref_1 = step5.ref_1
		and step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no ) 
		where step5.ref_1 is null
		--and  step4.doc_dt between  v_control_start_dt and v_control_end_dt ;
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Benefit');
	*/
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP41 Reconcile : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 
  	raise notice 'STEP41 % : % row(s) - Reconcile',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
  	begin
	  	-- Log.251
	  	-------- Exclude from dummy system_type_cd = 028
	 	delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm 
	 	where exists ( select 1 from dds.vw_ifrs_accrual_chg accru where accru.system_type_cd in ('028') and mm.nadet_policyno = accru.policy_no )
	 	and ref_1 like 'AC%' 
	 	and benefit_gmm_flg in ('8') ;
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	  	raise notice 'STEP42 % : % row(s) - Exclude from dummy',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
	    -------- Exclude from dummy system_type_cd = 029 
	 	delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm 
	 	where exists ( select 1 from dds.vw_ifrs_accrual_chg accru where accru.system_type_cd in ('029') and mm.nadet_policyno = accru.policy_no )
	 	and ref_1 like 'AC%' 
	 	and benefit_gmm_flg in ('2.1','2.2') ;
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	  	raise notice 'STEP42 % : % row(s) - Exclude from dummy',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP42 Reconcile : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
  
 
 	begin 
	 	-- Log.324 , 325 
	 	-------- Exclude from dummy system_type_cd = 014
		/*delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm 
		where left(ref_1,2) ='AC'
		and exists ( select 1
					from dds.ifrs_accrual_chg_202308 accru , dds.oic_natrn_chg natrn
					where accru.system_type_cd in ( '014','015')
					and accru.natrn_rk = natrn.natrn_rk 
					and mm.natrn_x_dim_rk = natrn_x_dim_rk );
		--and benefit_gmm_flg in ('8','1') ; 
		*/
	 	
	 	
		delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm  
		where left(ref_1,2) ='AC'
		and exists ( select 1
				from dds.vw_ifrs_accrual_chg accru , dds.oic_natrn_chg natrn
				where accru.system_type_cd in ( '014','015')
				and accru.natrn_rk = natrn.natrn_rk 
				and mm.natrn_x_dim_rk = natrn_x_dim_rk );
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	  	raise notice 'STEP42.1 % : % row(s) - Exclude from dummy 014,015',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	 	-- 07Dec2023 Narumol W. : Benefit Log 408 : Exclude accru dum_nt 
		delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm  
		where left(ref_1,2) ='AC'
		and benefit_gmm_flg = '8';
		/*
			and exists ( select 1
				from dds.vw_ifrs_accrual_chg accru  
				where accru.system_type_cd in ( '014','015')
				and accru.natrn_rk is null );
	 	*/
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	  	raise notice 'STEP42.2 % : % row(s) - Exclude from dummy 014,015',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	  
	    -- Log.382
		-- 10Aug2023 Narumol W : Log.382 เนื่องจากไม่มีเงื่อนไขในการ include LTB ใน stga_ifrs_nac_txn_step05_benefits จึงทำให้ ตก DUMMY ทั้งหมด จำเป็นต้อง Exclude ตั้งแต่ใน table : missing 
		-------- Exclude LTB 
		delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm 
		where exists ( select 1 from dds.oic_nac_chg nac 
			where  mm.branch_cd = nac.branch_cd
			and mm.doc_dt = nac.doc_dt
			and mm.doc_type = nac.doc_type
			and mm.doc_no = nac.doc_no 
			and mm.accode = nac.accode  
			and mm.natrn_dim_txt = nac.nac_dim_txt
			and nac.transaction_type = 'LTB'
			and nac.accode  = '5031010040');
		
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	  	raise notice 'STEP43 % : % row(s) - Exclude LTB from dummy',clock_timestamp()::varchar(19),v_affected_rows::varchar;
    
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_benefits' 				
				,'ERROR STEP42 Exclude from dummy 014,015 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 
  	/*
	-- Log.325
	-------- Exclude from dummy system_type_cd = 015
	delete from stag_a.stga_ifrs_nac_txn_missing_step05_benefits mm 
	where exists ( select 1 from dds.vw_ifrs_accrual_chg accru 
				where accru.system_type_cd = '015' 
				and mm.branch_cd = accru.branch_cd
				and mm.doc_dt = accru.doc_dt
				and mm.doc_type = accru.doc_type
				and mm.doc_no = accru.doc_no 
				and mm.accode = accru.accode)
	and ref_1 like 'AC%' ;
	--and accode in ( '5031010030','5031020030');
 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
  	raise notice 'STEP42 % : % row(s) - Exclude from dummy',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  	*/
  

 	------------------------------------------------

 	begin
		------------- DUMMY  
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select ref_1,nac_rk,natrn_x_dim_rk,branch_cd,sap_doc_type,reference_header,post_dt
		,doc_dt,doc_type,doc_no,accode,dc_flg,actype,nac_amt,posting_amt
		,system_type,transaction_type,premium_type,policy_no,plan_cd,rider_cd
		,pay_dt,pay_by_channel,pay_period,sum_natrn_amt,posting_sap_amt,posting_proxy_amt
		,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group
		,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
		,source_filename,group_nm,subgroup_nm,subgroup_desc
		,variable_cd,variable_nm,nadet_detail
		,org_branch_cd,org_submit_no,submit_no,section_order_no,is_section_order_no
		,for_branch_cd,event_type,benefit_gmm_flg,benefit_gmm_desc,policy_type
		,effective_dt,issued_dt,ifrs17_channelcode,ifrs17_partnercode
		,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable 
		from ( 
			select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
			,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
			,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
			,natrn.natrn_amt as nac_amt
			,natrn.posting_natrn_amt as posting_amt
			,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
			,natrn.nadet_policyno as policy_no,coalesce (natrn.plan_cd,pol.plan_cd) as plan_cd
			,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
			,null::varchar as pay_period
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,natrn.sales_id,natrn.sales_struct_n
			,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
			,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
			,natrn.filename as source_filename  
			, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
			, case when natrn.nadet_policyno is not null then coalesce (natrn.variable_cd,vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') else coa.dummy_variable_nm end as variable_cd
			, coalesce (vn1.variable_name,vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm
			, natrn.detail as nadet_detail 
			, natrn.org_branch_cd, null::varchar as org_submit_no
			, null::varchar as submit_no
			, null::varchar as section_order_no
			, 0 as is_section_order_no
			,natrn.for_branch_cd
			,natrn.event_type 
			,natrn.benefit_gmm_flg ,natrn.benefit_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
									order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,replace(vn.variable_cd,'V','')::int) as _rk 
			from  stag_a.stga_ifrs_nac_txn_missing_step05_benefits natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )  
			left join  stag_s.stg_tb_core_planspec pp 
			on ( natrn.plan_cd = pp.plan_cd) 
 			left outer join dds.tl_acc_policy_chg pol  
			on ( natrn.nadet_policyno = pol.policy_no
			and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm 
			and pol.policy_type not in ('M','G')) --Oil 25jan2023 : add pol.policy_type not in ('M','G')
			left outer join stag_s.ifrs_common_coa coa 
			on ( natrn.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn1  
		 	on ( vn1.event_type = 'Benefit' 
		 	and natrn.Benefit_gmm_flg  = vn1.Benefit_gmm_flg
			and natrn.variable_cd = vn1.variable_cd ) 
			left join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Benefit' 
			and natrn.Benefit_gmm_flg  = vn.Benefit_gmm_flg 
			)  
		where natrn.post_dt between v_control_start_dt and v_control_end_dt
		and  natrn.accode not in ('2013010020','2995020120')
		)  as aa 
		where _rk =1 ; 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_benefits' 				
				,'ERROR STEP42 VX Dummy: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP42 % : % row(s) - VX - Dummy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   
    
-- ===================================  V3 รายการจ่ายของเงินผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน =========================== -- 
    BEGIN
		select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg(v_control_start_dt) into out_err_cd;

		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		select  ivby.ref_1 as ref_1,ivby.nac_rk as nac_rk ,  natrn.natrn_x_dim_rk  
	  	, ivby.branch_cd ,natrn.sap_doc_type,natrn.reference_header , v_control_end_dt as  post_dt  --natrn.post_dt 
	  	, ivby.doc_dt,ivby.doc_type,ivby.doc_no
	  	, ivby.accode,ivby.dc_flg 
	  	, ivby.actype 
	  	, ivby.nac_amt 
	  	-- 03July2023 Narumol W. : [ITRQ#66062868] เปลี่ยนแปลง sign ของ CLAIM_YTD Var.6 และ BENEFIT Var.3
	  	, ivby.posting_amt  as posting_amt  -- Track_chg.71
	  	, ivby.system_type as system_type,ivby.transaction_type,ivby.premium_type 
	  	, ivby.policy_no ,ivby.plan_cd, ivby.rider_cd  
	  	, null::date as pay_dt ,ivby.pay_by_channel
	  	, ivby.pay_period
	  	, posting_natrn_amt as sum_natrn_amt ,ivby.posting_sap_amt,ivby.posting_proxy_amt   
	  	, ivby.sales_id, ivby.sales_struct_n, ivby.selling_partner,ivby.distribution_mode,ivby.product_group,ivby.product_sub_group,ivby.rider_group,ivby.product_term,ivby.cost_center 
	  	, ivby.nac_dim_txt
	  	, 'accrueac-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accruedac_current' as subgroup_nm , 'รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน'  as subgroup_desc 
	  	, vn.variable_cd ,vn.variable_name  
	  	, natrn.detail as nadet_detail ,ivby.org_branch_cd
	  	, ''::varchar as org_submit_no , ''::varchar as submit_no , ivby.section_order_no ,0 as is_section_order_no
	  	,natrn.for_branch_cd
	  	,coa.event_type  
	  	,coa.benefit_gmm_flg,coa.benefit_gmm_desc 
	  	,pol.policy_type ,pol.effective_dt ,pol.issued_dt
	  	,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
	  	,pp.ifrs17_portid ,pp.ifrs17_portgroup 
	  	,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
	  	from dds.ifrs_variable_benefits_ytd  ivby -- as 2022 >> 2021 dec-12-31
	  	inner join stag_s.ifrs_common_coa coa
	  	on ( ivby.accode = coa.accode )   
	  	left join stag_a.stga_ifrs_nac_txn_step04_eoy natrn 
	  	on ( natrn.branch_cd = ivby.branch_cd
	  	and natrn.doc_dt = ivby.doc_dt
	  	and natrn.doc_type = ivby.doc_type
	  	and natrn.doc_no = ivby.doc_no 
	  	and natrn.accode = ivby.accode
	  	and natrn.dc_flg = ivby.dc_flg 
	  	and natrn.natrn_dim_txt = ivby.nac_dim_txt
	   	) 
	  	left join  stag_s.stg_tb_core_planspec pp 
	  	on ( natrn.plan_cd = pp.plan_cd)
	  	left join stag_s.ifrs_common_variable_nm vn
	  	on ( vn.event_type = 'Benefit'
	  	and ivby.event_type = vn.event_type  -- Log.328
	  	and vn.variable_cd = 'V3'
	  	and vn.benefit_gmm_flg  = coa.benefit_gmm_flg  
	  	) 
	  	left outer join dds.tl_acc_policy_chg pol  
	  	on ( ivby.policy_no = pol.policy_no
	  	and  ivby.plan_cd = pol.plan_cd
	  	and ivby.doc_dt between pol.valid_val_fr_dttm and pol.valid_to_dttm )
	  	where  ivby.control_dt::timestamp = date_trunc('day',(date_trunc('year', v_control_end_dt::timestamp -interval '1 second')-interval '1 second')) 
	  	and ivby.variable_cd ='V2'
	  	and not exists ( select 1    -- ต้องอยู่หลัง V2 stag_a.stga_ifrs_nac_txn_step05_benefits
	        	from stag_a.stga_ifrs_nac_txn_step05_benefits  txn
	        	where ivby.policy_no = txn.policy_no 
	        	and ivby.accode = txn.accode 
	        	and coalesce(trim(ivby.plan_cd),'') = coalesce(trim(txn.plan_cd),'')
	        	and coalesce(trim(ivby.rider_cd),'') = coalesce(trim(txn.rider_cd),'')
	        	and ivby.section_order_no = txn.section_order_no 
	        	--and ivby.transaction_type = txn.transaction_type 
	        	and ivby.reference_header = txn.reference_header -- Log.268 
	        	and coalesce(ivby.pay_period,'') = coalesce(txn.pay_period,'') 
	        	and txn.variable_cd ='V2'
	        	)
		and coalesce(natrn.is_accru,0) <> 1 ;
		--and accru.system_type_cd <> '037'; -- Log 224  ยกเลิกเนื่องจาก ไม่มีข้อมูล system type 037 จาก Dec 
  		--and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP12 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP12 % : % row(s) - V3 รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	-- 03Oct2023 Narumol W. : [ITRQ#66104785] ขอปรับปรุงเงื่อนไข Coupon Dividend BENEFIT_YTD - Add Datasource apl_movement
 	if v_control_end_dt >= '2023-07-31'::date then
 	begin
		select dds.fn_dynamic_vw_ifrs_apl_movement(v_control_start_dt) into out_err_cd;

		insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		(post_dt,policy_no,plan_cd,transaction_type
		,nac_amt,posting_amt,pay_period,benefit_gmm_flg
		,variable_cd,variable_nm ,source_filename
		,group_nm,subgroup_nm,subgroup_desc)
		with tmp_apl as  ( 
		select v_control_end_dt as post_dt , apl.data_type
		,apl.policy_no , trim(apl.plan_cd) as plan_cd ,apl.nextperiod 
		,pd.dv as ndic_dv  
		,case when apl.data_type = 'Coupon' then 'paycond'
			when pd.dv  = 'DV02' then 'dividen_ndic'
			when pd.dv = 'DV03' then 'dividen_ins' end as transaction_type
		, coalesce(apl.return_amt ,0)  as return_amt
		,case when apl.data_type = 'Coupon' then '8'
		when pd.dv = 'DV01' then'2.1'
		when  pd.dv  = 'DV02' then '2.2'
		when pd.dv = 'DV03' then '2.1' end as benefit_gmm_flg
		,apl.source_filename  
		from dds.vw_ifrs_apl_movement apl 
		left outer join stag_s.stg_tb_ifrs_ndic_basic pd 
		on ( trim(apl.plan_cd) = pd.plan_cd )
		where coalesce(apl.return_amt ,0) > 0 ) 		 
		select post_dt
		,policy_no,plan_cd,transaction_type
		,return_amt  as nac_amt
		,return_amt*-1 as posting_amt 
		,nextperiod as pay_period
		,tmp_apl.benefit_gmm_flg
		,vn.variable_cd 
		,vn.variable_name 
		,source_filename 
		,CONCAT('APL Movement ',ndic_dv) as  group_nm
		, 'Accruedac_current' as subgroup_nm 
		, 'รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน'  as subgroup_desc 
		from tmp_apl 
		left outer join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type ='Benefit' 
		and tmp_apl.benefit_gmm_flg = vn.benefit_gmm_flg
		and vn.variable_cd='V3');
		
 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP12 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP12 % : % row(s) - V3 Add Datasource apl_movement',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	end if;
 
	--18Aug2023 Narumol W. [ITRQ#66083852] ขอเพิ่มเงื่อนไขของ V3 - Benefit_YTD
	--03Oct2023 Narumol W. [ITRQ#66104533] ขอเพิ่มเงื่อนไขของ V3 - Benefit_YTD : Exclude distribution_mode from key field 
     BEGIN
		
	    v_control_eoy_dt := (date_trunc('year',v_control_end_dt)-interval '1 day')::date;
	 	raise notice 'STEP13 % : % row(s) - v_control_eoy_dt',clock_timestamp()::varchar(19),v_control_eoy_dt::varchar;
    
	    insert into stag_a.stga_ifrs_nac_txn_step05_benefits 
		( branch_cd,reference_header,post_dt ,doc_dt
		,accode,posting_amt,policy_no,plan_cd,rider_cd
		,selling_partner ,product_group ,product_sub_group 
		,rider_group,product_term ,cost_center ,nac_dim_txt 
		,source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		,variable_cd ,variable_nm,event_type ,benefit_gmm_flg ,benefit_gmm_desc
		,policy_type ,effective_dt ,issued_dt 
		,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode 
		,ifrs17_portid ,ifrs17_portgroup 
		,is_duplicate_variable ,duplicate_fr_variable_nm  ) 
	    with benefits_eoy
		as ( select branch_cd ,accode ,policy_no ,plan_cd  ,rider_cd  ,variable_cd ,source_filename  
			, control_dt 
			,SUM(posting_amt) OVER(PARTITION BY branch_cd ,accode ,policy_no ,plan_cd ,rider_cd,reference_header
			,selling_partner ,product_group ,product_sub_group ,rider_group,product_term) AS posting_amt
			--03Oct2023 Narumol W. [ITRQ#66104533] ขอเพิ่มเงื่อนไขของ V3 - Benefit_YTD : Exclude distribution_mode from key field & summary field
			,concat(branch_cd,accode,policy_no,trim(plan_cd),trim(rider_cd),reference_header,selling_partner,product_group ,product_sub_group ,rider_group,product_term) as key_txt
			,reference_header ,selling_partner ,product_group ,product_sub_group ,rider_group,product_term  ,cost_center ,nac_dim_txt
			,event_type,benefit_gmm_flg,benefit_gmm_desc
			from dds.ifrs_variable_benefits_ytd 
			where control_dt = v_control_eoy_dt
			and accode = '5032010090'  
			and variable_cd ='V2' )  
		, benefits_current
		as ( select branch_cd ,accode ,policy_no ,plan_cd ,rider_cd  ,variable_cd ,source_filename 
			,SUM(posting_amt) OVER(PARTITION BY branch_cd ,accode ,policy_no ,plan_cd ,rider_cd,reference_header
			,selling_partner ,product_group ,product_sub_group ,rider_group,product_term) AS posting_amt
			--03Oct2023 Narumol W. [ITRQ#66104533] ขอเพิ่มเงื่อนไขของ V3 - Benefit_YTD : Exclude distribution_mode from key field 
			,concat(branch_cd,accode,policy_no,trim(plan_cd),trim(rider_cd),reference_header ,selling_partner ,product_group ,product_sub_group ,rider_group,product_term) as key_txt
			,reference_header ,selling_partner ,product_group ,product_sub_group ,rider_group,product_term 
			from stag_a.stga_ifrs_nac_txn_step05_benefits   
			where post_dt  between  v_control_start_dt and v_control_end_dt
			and accode = '5032010090'  
			and variable_cd ='V2' 
			)  
		select benefits_eoy.branch_cd, benefits_eoy.reference_header 
		,v_control_end_dt as post_dt ,v_control_end_dt as doc_dt 
		,benefits_eoy.accode  
		,benefits_eoy.posting_amt - benefits_current.posting_amt as posting_amt
		,benefits_eoy.policy_no,benefits_eoy.plan_cd,benefits_eoy.rider_cd
		,benefits_eoy.selling_partner ,benefits_eoy.product_group 
		,benefits_eoy.product_sub_group ,benefits_eoy.rider_group,benefits_eoy.product_term , benefits_eoy.cost_center 
		,benefits_eoy.nac_dim_txt
		,'benefits_ytd' as source_filename 
		,'benefits eoy - current' as group_nm 
		,'Accruedac_current' as subgroup_nm 	  	 
		,'รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน'  as subgroup_desc 
		,'V3' as variable_cd
		, vn.variable_name 
		, benefits_eoy.event_type, benefits_eoy.benefit_gmm_flg, benefits_eoy.benefit_gmm_desc
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from benefits_eoy inner join benefits_current
		on ( benefits_eoy.key_txt = benefits_current.key_txt)
		inner join stag_s.ifrs_common_variable_nm  vn 
		on (vn.event_type ='Benefit'
		and vn.variable_cd='V3'
		and benefits_eoy.benefit_gmm_flg = vn.benefit_gmm_flg )
		left join stag_s.stg_tb_core_planspec pp 
		on ( benefits_eoy.plan_cd = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		on ( benefits_eoy.policy_no = pol.policy_no
		and  benefits_eoy.plan_cd = pol.plan_cd
		and  benefits_eoy.control_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		where benefits_eoy.posting_amt - benefits_current.posting_amt < 0;
  
 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP12 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP12 % : % row(s) - V3 รายการจ่ายของผลประโยชน์ค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	---- YTD
	DELETE from stag_a.stga_ifrs_nac_txn_step05_benefits where variable_cd = 'V15';

	-- Exclude ไม่เอารายการ Unit Linked ที่ transaction_type = "LTB" ของ Ac code 5031010040 (No.11 Others (Loyalty Bonus) - INS) เข้า mapping variable 
	delete from stag_a.stga_ifrs_nac_txn_step05_benefits 
	where transaction_type  in ( 'LTB' )
	and accode = '5031010040'; 

	-- 02Oct2023 Narumol W. : Patch policy_no and plan_cd
	update stag_a.stga_ifrs_nac_txn_step05_benefits p
	set  policy_no = pol.policy_no 
	,plan_cd = pol.plan_cd
	from dds.tl_acc_policy_chg pol 
	where ( p.policy_no = pol.plan_cd || pol.policy_no 
	and p.doc_dt between pol.valid_fr_dttm  and pol.valid_to_dttm ) 
	and length(p.policy_no) = 12 
	and p.sap_doc_type in ('SI','SB');

	---------
 
	begin 
		
		select dds.fn_ifrs_accrual_benefit_release(v_control_end_dt) into out_err_cd  ;
	  
		insert into stag_a.stga_ifrs_nac_txn_step05_benefits
		( post_dt,doc_dt,accode,policy_no,plan_cd
		,variable_cd, variable_nm ,posting_amt
		,event_type,benefit_gmm_flg,benefit_gmm_desc
		,source_filename,group_nm,subgroup_nm,subgroup_desc,is_duplicate_variable)
		select r.release_dt , r.release_dt,r.accode,r.policy_no,r.plan_cd
		,r.variable_cd,r.variable_name,r.posting_amt
		,'Benefit' as even_type,r.benefit_gmm_flg,r.benefit_gmm_desc
		,'stag_a.stga_fn_ifrs_accrual_benefit_release' as source_filename
		,'Accrual release' as group_nm
		,'Accrual release' as subgroup_nm
		,r.migrate_case as subgroup_desc
		,0 as is_duplicate_variable 
		from  stag_a.stga_fn_ifrs_accrual_benefit_release r
		where release_dt = v_control_end_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 		raise notice 'STEP13 % : % row(s) - Accrual release',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP12 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	 
	-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
	begin 
		
		if date_part('month',v_control_start_dt) = 1  then  
		
			insert into stag_a.stga_ifrs_nac_txn_step05_benefits  
			select ref_1 ,nac_rk ,natrn_x_dim_rk ,branch_cd ,sap_doc_type ,reference_header
			,v_control_end_dt as post_dt 
			,doc_dt ,doc_type ,doc_no ,accode ,dc_flg ,actype ,nac_amt
			,posting_amt * -1 as posting_amt
			,system_type ,transaction_type ,premium_type ,policy_no ,plan_cd
			,rider_cd ,pay_dt ,pay_by_channel ,pay_period
			,sum_natrn_amt ,posting_sap_amt	,posting_proxy_amt ,sales_id
			,sales_struct_n,selling_partner,distribution_mode,product_group
			,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
			,source_filename,group_nm
			,'Accrued_bop' as subgroup_nm
			,subgroup_desc,variable_cd,variable_nm,nadet_detail,org_branch_cd
			,org_submit_no,submit_no,section_order_no,is_section_order_no
			,for_branch_cd,event_type,benefit_gmm_flg,benefit_gmm_desc,policy_type
			,effective_dt,issued_dt,ifrs17_channelcode,ifrs17_partnercode
			,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup
			,is_duplicate_variable,duplicate_fr_variable_nm 
			,fee_cd
			from  dds.ifrs_variable_benefits_ytd a
			where a.control_dt = v_control_end_dt - interval '1 month'
			and variable_nm in ('ACTUAL_BPHINS_CH_RDEXC');

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		end if;		
	
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.stga_ifrs_nac_txn_step05_benefits' 				
				,'ERROR STEP14 : VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด : '||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	
    raise notice 'STEP14 % : % row(s) - VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
	--=============== Insert into YTD ==================--
  	select dds.fn_ifrs_variable_benefits(p_xtr_start_dt) into out_err_cd  ;

  	---------------
    -- Complete
	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.fn_ifrs_variable_txn05_benefits','COMPLETE: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

	 
   	return i;
END

$function$
;
