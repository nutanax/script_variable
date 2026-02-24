CREATE OR REPLACE FUNCTION dds.fn_ifrs_variable_txn05_premium(p_xtr_start_dt date)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
-- =============================================  
-- Author:  Narumol W.
-- Create date: 2021-05-25
-- Description: For insert event data 
--  select dds.fn_ifrs_variable_txn05_premium('2023-12-20'::date)
-- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
-- 24Aug2022 Narumol W.: Track_chg no.32 ย้าย V8 ไป V7
-- 30Aug2022 Oil : Add condition dummy Log.201
-- 07Sept2022 Oil : update นำรายการที่เป็นของ M907 ออก Log.178  
-- 07Sep2022 Narumol W.:Premium Log 203 Exclude is_cutoff = 1 
-- 13Sep2022 Oil : Chenge source from  dds.ifrs_adth_pay_chg to dds.oic_adth_chg  Log.220    
-- 19Sep2022 Oil : change condition join from branch_cd to ref_branch_cd Log.204 
-- 11Nov2022 Oil : Case Policy doc_no 5 (Track change 47) 
-- 22Nov22 Narumol W. : TrackChg 50-51 Replace condition for GROUPEB is plan_cd = 798
-- 23Nov22 NarumolW. : change source from ifrs_adth_pay_chg to ifrs_adth_chg 
-- 30Nov22 Oil : add V1,V7 - บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน Track_chg 43
-- 15Dec2022 Oil : Log.254 Exclude product_group='17' and product_sub_group	='704' 
-- 18Jan2023 Narumol W.: Log.227 Use natrn.ref_1 instead of accru.ref1
-- 20Jan22 Narumol W.: GWP LogNo.9 N'Kate cf condition GEB to product_group=17 & product_sub_group=704
-- Oil 07Mar2023 : เพิ่ม join plan_cd Log340 อาจกระทบ log.199 (V7 payment_trans)
-- 13Feb2023 Narumol W. : comment nac.plancd & add pol.policy_type
-- Oil 2023-04-10 : add source ifrs_payment_reject_chg Log.356
-- Oil 20230516 : ITRQ-66052132 Add V13 stag_f.tb_sap_trial_balance  >> ปรับใช้ dds.tb_sap_trial_balance
-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
-- 07Jul2023 Narumol W. : ITRQ-66073307: เพิ่มเงื่อนไข V2,3 PREMIUM_YTD เบี้ยค้างรับจาก TLP เก่า
-- 18Jul2023 Narumol W. : Use stga_ifrs_nac_txn_step04_prem for run by monthly
-- 02Oct2023 Narumol W. : Patch policy_no and plan_cd
-- 29Oct2023 Narumol W. : Premium Log 394 
-- 09Jan2024 Narumol W. : fix Log.416 
-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap' 
-- 16Feb2024 Narumol W. : [ITRQ#67010275] Enhance GL_ACCOUNT_BALANCE_SEGMENT_GWP : Reject RID,RRID
-- 05Jun2024 Narumol W. : [ITRQ#67052429] Enhance Variable Premium Condition - V7 V9 V13
-- 05Jun2024 Narumol W. : [ITRQ#67052463]เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link (enhance)
-- 06Jul2024 Narumol W.: [ITRQ#67062844] Enhance Variable Premium Condition for Unit Linked Product 
-- 16Oct2024 Nuttadet O.: [ITRQ#67104925] ปรับเพิ่มเงื่อนไข Variable Premium Condition for Unit Linked Product (system_type = 'SC') 
-- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
-- 10Jun2025 Nuttadet O.: [ITRQ#68062184] Premium Variable - แก้ไขชื่อ table ใน V9 เนื่องจากมีการเปลี่ยน source '%newcase%' เป็น 'gl_transaction.newcase%'
-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
-- 14Jul2024 Nuttadet O. : [ITRQ#68062201] เพิ่ม call stored dds.fn_ifrs_variable_txn05_imo_premium
-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
-- =============================================  
declare 
	i int default  0;
	v_control_start_dt date;
	v_control_end_dt date; 
	out_err_cd int default 0;
	out_err_msg varchar(200);
	--out_cd void;

	v_affected_rows int default 0 ;
	v_diff_sec int;
	v_current_time timestamp;

	p_last_dt date;
	v_icg_tlp_id varchar(200);
begin 
	raise notice '-------- PREMIUM % : % row(s) - ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.fn_ifrs_variable_txn05_premium','START: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

	if p_xtr_start_dt is not null then 
		v_control_start_dt := date_trunc('month', p_xtr_start_dt - interval '1 month');
		v_control_end_dt := v_control_start_dt + interval '1 month -1 day' ;
	end if;

	raise notice 'v_control_start_dt :: % ', v_control_start_dt::VARCHAR(10);
	raise notice 'v_control_end_dt :: % ', v_control_end_dt::VARCHAR(10);

	insert into stag_s.stg_tb_core_planspec ( plan_cd , ifrs17_var_current , ifrs17_var_future , index_flg )
	select plan_cd , ifrs_var_current,ifrs17_var_future,plan_index
	from (   
	 select 'GEB1' as plan_cd ,'Y' as ifrs_var_current ,null as ifrs17_var_future,'TERM' as plan_index union all 
	 select 'GEB2' as plan_cd ,'Y' as ifrs_var_current ,null as ifrs17_var_future,'TERM' as plan_index  union all 
	 select 'PL34' as plan_cd ,'Y' as ifrs_var_current ,'Y' as ifrs17_var_future ,'TERM' as plan_index   union all 
	 select 'M907' as plan_cd ,'Y' as ifrs_var_current ,'Y' as ifrs17_var_future ,'TERM' as plan_index   
	 ) pl   
	 where plan_cd not in ( select plan_cd from stag_s.stg_tb_core_planspec  );

	begin 
	-- Prepare accrual 
  	raise notice '--------Start fn_dynamic_vw_ifrs_accrual_chg  %',clock_timestamp()::varchar(19) ;
	select dds.fn_dynamic_vw_ifrs_accrual_chg (v_control_start_dt , v_control_end_dt) into out_err_cd;
	select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg (v_control_start_dt ) into out_err_cd;
 	raise notice '--------END fn_dynamic_vw_ifrs_accrual_eoy_chg  %',clock_timestamp()::varchar(19) ;
 
 	truncate table stag_a.stga_ifrs_nac_txn_step04_prem ;
 
 	insert into stag_a.stga_ifrs_nac_txn_step04_prem
 	select * 
 	,date_trunc('month', post_dt )::date as control_dt
 	from stag_a.stga_ifrs_nac_txn_step04 
 	where post_dt  between v_control_start_dt and v_control_end_dt
 	and accode  in ( select accode from stag_s.ifrs_common_coa where event_type ='Premium' );
 
 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
	raise notice 'STEP01 % : % row(s) - stga_ifrs_nac_txn_step04_prem',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP1 Call fn_dynamic_vw_ifrs_accrual_chg : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  

 	--  select * from dds.tb_fn_log order by log_dttm desc  
	truncate table stag_a.stga_ifrs_nac_txn_step05_premium; 

	-- 14Jul2024 Nuttadet O. : [ITRQ#68062201] เพิ่ม call stored dds.fn_ifrs_variable_txn05_imo_premium
  	select dds.fn_ifrs_variable_txn05_imo_premium(p_xtr_start_dt) into out_err_cd  ;

	-----------------------  EVENT V11 - Accru
	begin 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accrued Ac APL' as subgroup_nm , 'บัญชีเงินกู้กรมธรรม์ APL'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn 
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
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V11'
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		where accru.doc_dt between v_control_start_dt and v_control_end_dt
		 and natrn.reference_header in ( 'ACCAPL' ) ;
		-- and coalesce(natrn.is_reverse,0) <> 1;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP1 V11 Accrued Ac APL : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP01 % : % row(s) - V11 Accrued Ac APL',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accrued Ac APL' as subgroup_nm , 'บัญชีเงินกู้กรมธรรม์ APL'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from dds.vw_ifrs_accrual_chg accru -- stag_a.stga_ifrs_accrual accru -- select * from dds.vw_ifrs_accrual_chg
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn -- select * from stag_a.stga_ifrs_nac_txn_step04_eoy
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
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V12'
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		where accru.doc_dt between v_control_start_dt and v_control_end_dt
		 and natrn.reference_header in (  'ACCCPL','ACCCUT') ;
		 --and coalesce(natrn.is_reverse,0) <> 1; 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP02 V12 Accrued Ac APL : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP02 % : % row(s) - V12 Accrued Ac APL',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	---------------------- EVENT 06 

	--- EVENT 06 - Claim pos advance 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amt as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename , 'nac-claim' as  group_nm  , 'claim' as subgroup_nm ,  'claim pos advance' as subgroup_desc 
		,vn.variable_cd
		,vn.variable_name as variable_nm  
		,natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable  
		from dds.ifrs_claim_advance_x_nac nac  
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		on ( natrn.natrn_x_dim_rk =  nac.natrn_x_dim_rk)
		left join stag_s.ifrs_common_accode acc 
		on ( nac.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium'
		and vn.variable_cd = 'V1' -- track_chg no.31 ย้าย V6 ไป V1
		 and natrn.event_type = vn.event_type
		 and natrn.premium_gmm_flg = vn.premium_gmm_flg 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where --natrn.premium_gmm_flg <> '4'
		natrn.branch_cd <> '000' --Log.67
		--and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn where txn.nac_rk = nac.nac_rk and txn.variable_cd = 'V1' )
		and natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP03 V1 CLAIM POS ADVANCE: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP03 % : % row(s) - V1 CLAIM POS ADVANCE',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	

	BEGIN		
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk 
		,nac.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		, nac.filename as source_filename  , 'natrn-nac' as  group_nm  , 'claim' as subgroup_nm , 'claim'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem  as natrn -- select * from stag_a.stga_ifrs_nac_txn_step04 where event_type = 'Premium' and premium_gmm_flg = '1'
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn  
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1' -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		where (dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false )  
		-- and   natrn.premium_gmm_flg = '1' 
		and natrn.branch_cd <> '000' --Log.67
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn where txn.nac_rk = nac.nac_rk and txn.variable_cd = 'V1' )  
		and natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP04 V1 CLAIM : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP04 % : % row(s) - V1 CLAIM',clock_timestamp()::varchar(19),v_affected_rows::varchar;

		--- EVENT 06 - CLAIM POS BRANCH
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename , 'nac-claim' as  group_nm  , 'claim' as subgroup_nm ,  'claim pos cash Branch' as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		,natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from dds.oic_nac_chg nac 
		inner join stag_a.stga_ifrs_nac_txn_step04_prem  natrn
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join stag_s.ifrs_common_accode  acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium'
		and vn.variable_cd = 'V1'  -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.source_type = 'B'
		and natrn.event_type  = 'Premium'
		and natrn.branch_cd <> '000' --Log.67 
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn where txn.nac_rk = nac.nac_rk and txn.variable_cd = 'V1' )
		and exists (  select 1 
		                from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2' 
		                 and adth.account_flg = 'C'
		                 and adth.branch_cd = nac.branch_cd  
		                 and adth.ref_doc_dt = nac.doc_dt  
		                 and adth.ref_doc_no = nac.doc_no 
		                 and adth.policy_no = nac.policy_no --Log.67
		                 )  
		and natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP05 V1 CLAIM POS BRANCH: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP05 % : % row(s) - V1 CLAIM POS BRANCH',clock_timestamp()::varchar(19),v_affected_rows::varchar;
				
		--- EVENT 06 - Claim pos cash HQ
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename , 'nac-claim' as  group_nm  , 'claim' as subgroup_nm ,  'claim POS HQ' as subgroup_desc 
		,vn.variable_cd
		,vn.variable_name as variable_nm 
		,natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from dds.oic_nac_chg nac 
		inner join stag_a.stga_ifrs_nac_txn_step04_prem  natrn   
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join stag_s.ifrs_common_accode  acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no = pol.policy_no
		 and nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium'
		and vn.variable_cd = 'V1'  -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.branch_cd <> '000' --Log.67
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
						where txn.nac_rk = nac.nac_rk and txn.variable_cd in ( 'V1', 'V6' ) ) 
		and natrn.premium_gmm_flg <> '4'
		and exists (  select 1 
		                from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2' 
		                 --and adth.account_flg in ('C','Y')
		                 and adth.source_filename in ('mgadth', 'adthtran')
		                 and adth.branch_cd = nac.branch_cd  
		                 and adth.ref_doc_dt = nac.doc_dt  
		                 and adth.ref_doc_no = nac.doc_no  
		                 and adth.policy_no = nac.policy_no  -- Log.67
		                 )   
		and natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP06 V1 CLAIM POS HQ: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP06 % : % row(s) - V1 CLAIM POS HQ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
				 	 	
		--- EVENT 06 - Claim NATRN-adth
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , adth.adth_rk  , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,adth.ref_doc_dt doc_dt,adth.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.posting_claim_amt as posting_sum_nac_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd  ,null::date pay_dt ,null::varchar pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,adth.adth_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem  natrn
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
		left outer join dds.tl_acc_policy_chg pol  
		 on (  adth.policy_no = pol.policy_no
		 and  adth.plan_cd = pol.plan_cd
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'  -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
		 where  natrn.event_type = 'Premium'
		 and natrn.branch_cd <> '000' --Log.67
		 and not exists ( select 1 from dds.oic_nac_chg nac
							where natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
							and natrn.doc_dt = nac.doc_dt 
							and natrn.doc_type = nac.doc_type
							and natrn.doc_no = nac.doc_no 
							and natrn.accode = nac.accode  
							and natrn.dc_flg = nac.dc_flg			
							and nac.nac_dim_txt  = natrn.natrn_dim_txt )   
		and natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP07 V1 CLAIM: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP07 % : % row(s) - V1 CLAIM',clock_timestamp()::varchar(19),v_affected_rows::varchar;		
 
 	---------------------- EVENT 01 select * from stag_s.stg_tb_core_planspec
	BEGIN 
	 
		--nuttadet.oras 20250909
 		truncate table stag_a.stga_ifrs_premium_temp  ;

		insert into stag_a.stga_ifrs_premium_temp
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt  
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename  , 'nac'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		inner join dds.oic_nac_chg  nac -- dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		-- 16Jun2023 narumol W. : ITRQ66052190 Issue log 
		--on (  natrn.branch_cd  = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  nac.policy_no = pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		--where natrn.post_dt  between v_control_start_dt and v_control_end_dt
		where natrn.control_dt =  v_control_start_dt
		and (replace(nac.submit_no,' ','') = '' 
			or dds.fn_is_numeric(coalesce(trim(replace(nac.submit_no,' ','')),'0'))  is true 
			or nac.submit_no like 'OIC%' ) ;  

		insert into stag_a.stga_ifrs_nac_txn_step05_premium 			
		select * from stag_a.stga_ifrs_premium_temp nac 
		where not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
		                   where txn.nac_rk = nac.nac_rk and txn.variable_cd= 'V1'); -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
			
		--nuttadet.oras 20250909
		
		/* nuttadet.oras 20250909

		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt  
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename  , 'nac'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		inner join dds.oic_nac_chg  nac -- dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		-- 16Jun2023 narumol W. : ITRQ66052190 Issue log 
		--on (  natrn.branch_cd  = nac.branch_cd
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  nac.policy_no = pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		--where natrn.post_dt  between v_control_start_dt and v_control_end_dt
		where natrn.control_dt = v_control_start_dt
		and (replace(nac.submit_no,' ','') = '' 
			or dds.fn_is_numeric(coalesce(trim(replace(nac.submit_no,' ','')),'0'))  is true 
			or nac.submit_no like 'OIC%' )
		/*and ( case when replace(nac.submit_no,' ','') = '' then true 
					else  dds.fn_is_numeric(coalesce(trim(replace(nac.submit_no,' ','')),'0')) end  is true 
			or nac.submit_no like 'OIC%' )  */
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
						where txn.nac_rk = nac.nac_rk and txn.variable_cd= 'V1'); -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
		/*
		and not exists ( select 1 from dds.oic_unwtoacc_chg unw  
						where nac.branch_cd = unw.branch_cd  
						and nac.doc_dt = unw.doc_dt  
						and nac.doc_type = unw.doc_type  
						and nac.doc_no = unw.doc_no   ) 
		and not exists ( select 1 from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2' 
		                 and adth.account_flg = 'C'
		                 and adth.branch_cd = natrn.branch_cd  
		                 and adth.ref_doc_dt = natrn.doc_dt  
		                 and adth.ref_doc_no = natrn.doc_no
		                 and adth.policy_no = nac.policy_no --Log.67
		                 );    	                 
		*/
	 
		nuttadet 20250909  */				
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP08.1 % : % row(s) - V1 accounting transaction',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		delete from stag_a.stga_ifrs_nac_txn_step05_premium nac
		where variable_cd ='V1' 
		and subgroup_nm = 'accounting transaction'
		and exists ( select 1 from dds.oic_unwtoacc_chg unw  
						where nac.branch_cd = unw.branch_cd  
						and nac.doc_dt = unw.doc_dt  
						and nac.doc_type = unw.doc_type  
						and nac.doc_no = unw.doc_no   ) ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP08.2 % : % row(s) - V1 Exclude unw ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		delete from stag_a.stga_ifrs_nac_txn_step05_premium nac
		where variable_cd ='V1' 
		and subgroup_nm = 'accounting transaction'
		and exists  ( select 1 from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2' 
		                 and adth.account_flg = 'C'
		                 and adth.branch_cd = nac.branch_cd  
		                 and adth.ref_doc_dt = nac.doc_dt  
		                 and adth.ref_doc_no = nac.doc_no
		                 and adth.policy_no = nac.policy_no --Log.67
		                 );    	   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP08.3 % : % row(s) - V1 Exclude adth pos ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
				
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP08 V1 accounting transaction : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP08 % : % row(s) - V1 accounting transaction',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	---------------------- EVENT 01

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
	, row_number() over  ( partition by natrn_rk,refund_x_dim_rk,branch_cd , doc_dt ,doc_type ,doc_no ,accode ,policy_no ,rider_cd ,check_sum
	order by natrn_rk,refund_x_dim_rk,branch_cd , doc_dt ,doc_type ,doc_no ,accode ,policy_no ,rider_cd ,check_sum,valid_from_dttm desc ) as _rk 
	from dds.ifrs_refund_trans_chg   
	) as a 
	where _rk = 1 ;

	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	raise notice 'STEP09 % : % row(s) - Prep ifrs_refund_trans_chg_no_dup',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select natrn.ref_1 as ref_1 ,refund.refund_trans_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, refund.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, refund.doc_dt , refund.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, refund.posting_refund_amt   as posting_amt
		, refund.system_type ,refund.transaction_type,refund.premium_type 
		, refund.policy_no as policy_no ,refund.plan_cd as plan_cd , refund.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, refund.sales_id,refund.sales_struct_n,refund.selling_partner,refund.distribution_mode,refund.product_group,refund.product_sub_group,refund.rider_group,refund.product_term,refund.cost_center
		, refund.refund_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-refund' as  group_nm  , 'Premium discount' as subgroup_nm ,'ส่วนลดเบี้ย'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,refund.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,refund.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		inner join stag_a.ifrs_refund_trans_chg_no_dup refund -- dds.ifrs_refund_trans_chg  refund  
		on ( natrn.branch_cd =  refund.branch_cd 
		and natrn.doc_dt = refund.doc_dt
		and natrn.doc_type =  refund.doc_type 
		and natrn.doc_no = refund.doc_no 
		and  natrn.dc_flg = refund.dc_flg
		and natrn.accode = refund.accode 
		and natrn.natrn_dim_txt = refund.refund_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( refund.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  refund.policy_no = pol.policy_no
		 and  refund.plan_cd = pol.plan_cd
		 and refund.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
		and natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP09 V1 Premium discount: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP09 % : % row(s) - V1 -Premium discount',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
			
	---------------------- EVENT 01 - nacpaylink
	begin 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt -- posting_sum_nac_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename  , 'natrn-nacpaylink'::varchar(100) as  group_nm , 'nacpaylink'::varchar(100) as subgroup_nm , 'แฟ้มข้อมูลการจ่ายเงิน เฉพาะจ่ายเช็ค , เงินปันผล เริ่มใช้ 2563' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_acctbranch_nacpaylink_chg nac 
		on (  natrn.natrn_x_dim_rk = nac.natrn_x_dim_rk )   
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  nac.policy_no = pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		where natrn.control_dt = v_control_start_dt ; 

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP10 V1 nacpaylink : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP10 % : % row(s) - V1 -nacpaylink',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	---------------------- EVENT 01 - Payment Trans
	begin 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select natrn.ref_1 as ref_1 ,refund.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, refund.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, refund.doc_dt , refund.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, refund.posting_refund_amt   as posting_amt
		, refund.system_type ,refund.transaction_type,refund.premium_type 
		, refund.policy_no as policy_no ,coalesce (pol.plan_cd ,refund.plan_cd) as plan_cd , refund.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,refund.sales_id,refund.sales_struct_n,refund.selling_partner,refund.distribution_mode,refund.product_group,refund.product_sub_group,refund.rider_group,refund.product_term,refund.cost_center
		,refund.transpos_dim_txt as refund_dim_txt  
		, 'payment_trans'::varchar as source_filename  , 'natrn-payment_trans' as  group_nm  , 'Premium discount' as subgroup_nm ,'ส่วนลดเบี้ย'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,refund.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,refund.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		inner join dds.ifrs_payment_transpos_chg  refund  
		on ( natrn.org_branch_cd =  refund.ref_branch_cd  --Log.204 Oil 19Sep2022 : change join branch_cd >> ref_branch_cd
		and natrn.doc_dt = refund.doc_dt
		and natrn.doc_type =  refund.doc_type 
		and natrn.doc_no = refund.doc_no 
		and natrn.dc_flg = refund.dc_flg
		and natrn.accode = refund.accode 
		and natrn.natrn_dim_txt = refund.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
 		left outer join dds.tl_acc_policy_chg pol  
		 on (  refund.policy_no = pol.policy_no 
		 and refund.plan_cd = pol.plan_cd -- Oil 07Mar2023 : เพิ่ม join plan_cd Log340 อาจกระทบ log.199 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP11 V1 payment_trans : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP11 % : % row(s) - V1 -payment_trans',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	-- Log.145 :Add New Condition V1: เบี้ยที่มาจาก system CLAIMPOS และ branch = สนญ. H00 ให้เข้า V1
	BEGIN 
	 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt  
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm 
		, 'CLAIMPOS HQ'::varchar(100) as subgroup_nm , 'CLAIMPOS HQ'::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		inner join dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  nac.policy_no = pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		where nac.system_type  = 'CLAIMPOS'
		and nac.branch_cd  = '000'
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
						where txn.nac_rk = nac.nac_rk and txn.variable_cd in ( 'V1' ))  -- 24Aug2022 Narumol W.: Track_chg no.31 ย้าย V6 ไป V1
		and natrn.control_dt = v_control_start_dt ;
/*		and not exists ( select 1 from dds.oic_unwtoacc_chg unw  
						where nac.branch_cd = unw.branch_cd  
						and nac.doc_dt = unw.doc_dt  
						and nac.doc_type = unw.doc_type  
						and nac.doc_no = unw.doc_no   ) 
		and not exists ( select 1 from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2' 
		                 and adth.account_flg = 'C'
		                 and adth.branch_cd = natrn.branch_cd  
		                 and adth.ref_doc_dt = natrn.doc_dt  
		                 and adth.ref_doc_no = natrn.doc_no
		                 and adth.policy_no = nac.policy_no --Log.67
		                 )    	                 
	*/	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP12.1 % : % row(s) - V1 accounting transaction',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 
		-- 05May2024 Narumol W. : [ITRQ#67052181]Enhance Premium V1 เพิ่ม condition กรองข้อมูลรายการงานสินไหมจากแฟ้ม NAC
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt  
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm 
		, 'CLAIMPOS HQ'::varchar(100) as subgroup_nm , 'CLAIMPOS HQ'::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		inner join dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd  
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  nac.policy_no = pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	
		where nac.branch_cd = '000' 
		and natrn.product_group = '14'
		and nac.transaction_type ='CLM' 
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
						where txn.nac_rk = nac.nac_rk and txn.variable_cd in ( 'V1' ))   
		and natrn.control_dt = v_control_start_dt ; 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP12.1 % : % row(s) - V1 CLAIMPOS HQ UL',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		delete from stag_a.stga_ifrs_nac_txn_step05_premium nac
		where variable_cd ='V1' 
		and subgroup_nm = 'CLAIMPOS HQ'
		and exists ( select 1 from dds.oic_unwtoacc_chg unw  
						where nac.branch_cd = unw.branch_cd  
						and nac.doc_dt = unw.doc_dt  
						and nac.doc_type = unw.doc_type  
						and nac.doc_no = unw.doc_no   ) ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP12.2 % : % row(s) - V1 Exclude unw ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	 
		delete from stag_a.stga_ifrs_nac_txn_step05_premium nac
		where variable_cd ='V1' 
		and subgroup_nm = 'CLAIMPOS HQ'
		and exists  ( select 1 from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2' 
		                 and adth.account_flg = 'C'
		                 and adth.branch_cd = nac.branch_cd  
		                 and adth.ref_doc_dt = nac.doc_dt  
		                 and adth.ref_doc_no = nac.doc_no
		                 and adth.policy_no = nac.policy_no --Log.67
		                 );    	   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP12.3 % : % row(s) - V1 Exclude adth pos ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
					
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP12 V1 accounting transaction : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
			
   --Oil 2023-04-10 : add source ifrs_payment_reject_chg Log.356
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
        , reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
        , reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
        , natrn.accode , natrn.dc_flg , null::varchar as actype
        --, reject.reject_amt 
        -- 29Oct2023 Narumol W.: Premium Log 394 
        , reject.reject_posting_amt as posting_amt
        , reject.system_type ,reject.transaction_type,null::varchar as premium_type 
        , reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
        --,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        , natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
        , reject.natrn_dim_txt as refund_dim_txt  
        , 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  , 'Reject Premium' as subgroup_nm ,'Reject กลับรายการโอนเงินไม่สำเร็จ'  as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm 
        , natrn.detail as nadet_detail 
        ,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
        ,reject.for_branch_cd
        ,natrn.event_type 
        ,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable --,vn.duplicate_fr_variable_nm 
        from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
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
        on ( vn.event_type = 'Premium'
        and vn.variable_cd = 'V1'  
        and natrn.premium_gmm_flg = vn.premium_gmm_flg
        and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) ) 
        where natrn.control_dt = v_control_start_dt ;
        --where  natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP13 V1 Payment_reject: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;  
	raise notice 'STEP13 % : % row(s) - V1 Payment Reject',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
	-- V1 Payment_api_reverse 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
 		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk
 		, natrn.natrn_x_dim_rk  
        , reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
        , reject.doc_dt,reject.doc_type,reject.doc_no
        , natrn.accode, natrn.dc_flg , null::varchar as actype
        , reject.posting_payment_api_reverse_amt as posting_amt
        , reject.system_type ,reject.transaction_type,null::varchar as premium_type 
        , reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
        , null::date as pay_dt ,''::varchar as pay_by_channel
        , posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        , natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
        , reject.product_group , reject.product_sub_group ,reject.rider_group ,reject.product_term 
        , reject.cost_center , reject.reverse_dim_txt as refund_dim_txt  
        , 'paymentapi_reverse'::varchar as source_filename , 'natrn-paymentapi_reverse' as group_nm 
        , 'paymentapi_reverse Premium' as subgroup_nm ,'PAYMENT API Reverse กลับรายการโอนเงินไม่สำเร็จ' as subgroup_desc         
        , vn.variable_cd , vn.variable_name as variable_nm  
        , natrn.detail as nadet_detail , reject.branch_cd as org_branch_cd 
        , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no 
        , 0 as is_section_order_no , reject.for_branch_cd , natrn.event_type  , natrn.premium_gmm_flg 
        , natrn.premium_gmm_desc , pol.policy_type ,pol.effective_dt ,pol.issued_dt
        , pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        , pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
        from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
        inner join  dds.ifrs_payment_api_reverse_chg  reject  
        on ( natrn.branch_cd =  reject.branch_cd 
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
        on ( vn.event_type = 'Premium'
        and vn.variable_cd = 'V1'  
        and natrn.premium_gmm_flg = vn.premium_gmm_flg
        and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) ) 
        where natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP13 V1 Payment_api_reverse: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;  
	raise notice 'STEP13 % : % row(s) - V1 Payment_api_reverse',clock_timestamp()::varchar(19),v_affected_rows::varchar;


   
		 ---------------------- EVENT 02 'Accrued Ac'
	-- Oil 28Dec2022 : cheng subgroup_nm to 'Accruedac_current' 
	begin
		
	    /* nuttadet 2025-09-09 	 close	
	     
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accruedac_current' as subgroup_nm , 'เบี้ยที่เกิดจากการตั้งค้างรับ'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
		from dds.vw_ifrs_accrual_chg accru  -- select * from  dds.vw_ifrs_accrual_chg limit 10 
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 		 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V2' 
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where  natrn.control_dt = v_control_start_dt
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
						where txn.nac_rk = accru.accrual_rk and txn.variable_cd in ( 'V11','V12' ))
		and natrn.reference_header  not in ('ACCCPL','ACCCUT','ACCAPL' ) ;--Chg. 18
		--and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
	 
		nuttadet 2025-09-09  close */
		
		/* nuttadet 2025-09-09 add stag_f */
		truncate table stag_a.stga_ifrs_premium_temp1 ;
			
	    insert into stag_a.stga_ifrs_premium_temp1
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accruedac_current' as subgroup_nm , 'เบี้ยที่เกิดจากการตั้งค้างรับ'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
		from dds.vw_ifrs_accrual_chg accru  -- select * from  dds.vw_ifrs_accrual_chg limit 10 
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 		 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V2' 
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where  natrn.control_dt = v_control_start_dt
	    and natrn.reference_header  not in ('ACCCPL','ACCCUT','ACCAPL' ) ; 
	   
	   
	   insert into stag_a.stga_ifrs_nac_txn_step05_premium  
	   select * from stag_a.stga_ifrs_premium_temp1 accru
	   where not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
						  where txn.nac_rk = accru.nac_rk and txn.variable_cd in ( 'V11','V12' ));
	    
		/* nuttadet 2025-09-09 add stag_f */		
		
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP13 V2 ACCRUED: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP13 % : % row(s) - V2 ACCRUED',clock_timestamp()::varchar(19),v_affected_rows::varchar;		

	-- 07Jul2023 Narumol W. : ITRQ-66073307: เพิ่มเงื่อนไข V2,3 PREMIUM_YTD เบี้ยค้างรับจาก TLP เก่า
	begin
		
		select subgroup_id 
		into v_icg_tlp_id
		from dds.ifrs_icg_coverage_master_tlp 
		where plan_cd = '_TLP';
		raise notice 'v_icg_tlp_id :: % ', v_icg_tlp_id::VARCHAR(100);

		-- เบี้ยที่เกิดจากการตั้งค้างรับ จะมีการส่งทุกเดือนเฉพาะของเดือนปัจจุบันเท่านั้น
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode , dc_flg ,   posting_amt 
		 ,policy_no ,plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm, event_type ,premium_gmm_flg ,premium_gmm_desc
		, rider_cd) 
 		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  ,posting_sap_amt  
		, gl.policy_no 
		, gl.plan_cd 
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		,'SAP' as source_filename 		
		, 'SAP-TLP'::varchar(100) as  group_nm , 'Accruedac_current' as subgroup_nm , 'เบี้ยที่เกิดจากการตั้งค้างรับ จะมีการส่งทุกเดือนเฉพาะของเดือนปัจจุบันเท่านั้น' ::varchar(100) as subgroup_desc  
		, v_icg_tlp_id as variable_cd  
        , vn.variable_name as variable_nm  
		, coa.event_type , coa.premium_gmm_flg ,coa.premium_gmm_desc 
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd) 
		from stag_a.stga_ifrs_nac_txn_step01 gl 
		inner join stag_s.ifrs_common_coa coa 
		on ( gl.account_no = coa.accode  )     
        left outer join stag_s.ifrs_common_variable_nm vn 
        on ( coa.event_type = vn.event_type 
        and coa.premium_gmm_flg  = vn.premium_gmm_flg
        and vn.variable_cd = 'V2'
        and vn.ifrs17_var_future = 'Y' ) 
        where coa.event_type  = 'Premium' 
		and gl.doc_type = 'SA' 
		and gl.plan_cd  = '_TLP'
		and gl.assignment_code ='A' 
		--and gl.posting_date_dt between '2023-01-01' and '2023-01-31'  
		and posting_date_dt  between v_control_start_dt and v_control_end_dt; 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP13 V2 ACCRUED: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP13 % : % row(s) - V2 ACCRUED',clock_timestamp()::varchar(19),v_affected_rows::varchar;		


		 ---------------------- EVENT 03  จะถูกส่งข้อมูลของยอดสิ้นเดือน ธ.ค.ปีที่แล้วในเดือนม.ค.เท่านั้น 
	begin
		/*
		if date_part('month',v_control_start_dt) = 1  then 
			select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg(v_control_start_dt) into out_err_cd;
		end if;
		*/
		if date_part('month',v_control_start_dt) = 1  then 
			insert into stag_a.stga_ifrs_nac_txn_step05_premium  
			select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
			, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,v_control_end_dt as post_dt
			, accru.doc_dt,accru.doc_type,accru.doc_no
			, accru.accode,accru.dc_flg 
			, accru.ac_type 
			, accru.posting_accru_amount*-1  as posting_amt --Log.114
			, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
			, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
			, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
			, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
			, accru.accru_dim_txt
			, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accrued Ac' as subgroup_nm , 'เบี้ยที่เกิดจากการตั้งค้างรับ'  as subgroup_desc 
			, vn.variable_cd
			, vn.variable_name as variable_nm  
			, natrn.detail as nadet_detail ,accru.org_branch_cd
			, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
			,natrn.for_branch_cd
			,natrn.event_type 
			,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
			from  dds.vw_ifrs_accrual_eoy_chg accru 
			inner join stag_a.stga_ifrs_nac_txn_step04_eoy natrn 
			on ( natrn.branch_cd = accru.branch_cd
			and natrn.doc_dt = accru.doc_dt
			and natrn.doc_type = accru.doc_type
			and natrn.doc_no = accru.doc_no  
			and natrn.dc_flg = accru.dc_flg
			and natrn.accode = accru.accode
			and natrn.natrn_dim_txt =  accru.accru_dim_txt
			 )
			left join  stag_s.stg_tb_core_planspec pp 
			on (accru.plan_code = pp.plan_cd)
			left outer join dds.tl_acc_policy_chg pol  
			on (  accru.policy_no = pol.policy_no
			and  accru.plan_code = pol.plan_cd
			and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			inner join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Premium'
			and vn.variable_cd = 'V3' 
			and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
			where natrn.reference_header not in ('ACCCPL','ACCCUT','ACCAPL') ;--Chg. 18 
			
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP14 % : % row(s) - V3 - END OF YEAR',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
			-- 07Jul2023 Narumol W. : ITRQ-66073307: เพิ่มเงื่อนไข V2,3 PREMIUM_YTD เบี้ยค้างรับจาก TLP เก่า
			-- ล้างเบี้ยที่เกิดจากการตั้งค้างรับ ส่งในเดือนมกราคมปีปัจจุบัน ครั้งเดียว 
			insert into stag_a.stga_ifrs_nac_txn_step05_premium 
			( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
			, doc_dt ,accode , dc_flg ,   posting_amt 
			 ,policy_no ,plan_cd 
			, posting_sap_amt ,posting_proxy_amt  
			, selling_partner,distribution_mode,product_group
			, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
			, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
			, variable_cd ,variable_nm, event_type ,premium_gmm_flg ,premium_gmm_desc
			, rider_cd) 
	 		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
			, doc_dt ,account_no as sap_acc_cd ,dc_flg  ,posting_sap_amt  
			, gl.policy_no 
			, gl.plan_cd 
			, posting_sap_amt ,posting_sap_amt 
			, gl.selling_partner,gl.distribution_mode,gl.product_group
			, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
			,'SAP' as source_filename 		
			, 'SAP-TLP'::varchar(100) as  group_nm , 'SAP TLP' as subgroup_nm , 'ล้างเบี้ยที่เกิดจากการตั้งค้างรับ ส่งในเดือนมกราคมปีปัจจุบันครั้งเดียว' ::varchar(100) as subgroup_desc  
			, v_icg_tlp_id as variable_cd  
	        , vn.variable_name as variable_nm  
			, coa.event_type , coa.premium_gmm_flg ,coa.premium_gmm_desc 
			, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd) 
			from stag_a.stga_ifrs_nac_txn_step01 gl 
			inner join stag_s.ifrs_common_coa coa 
			on ( gl.account_no = coa.accode  )     
	        left outer join stag_s.ifrs_common_variable_nm vn 
	        on ( coa.event_type = vn.event_type 
	        and coa.premium_gmm_flg  = vn.premium_gmm_flg
	        and vn.variable_cd = 'V2'
	        and vn.ifrs17_var_future = 'Y' ) 
	        where coa.event_type  = 'Premium' 
			and gl.doc_type = 'SX' 
			and gl.plan_cd  = '_TLP'
			and gl.assignment_code ='A'  
			and posting_date_dt  between v_control_start_dt and v_control_end_dt; 
			
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP14 % : % row(s) - V3 - SAP TLP ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
		end if;
		
	 		EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP14 V3 ACCRUED-END OF YEAR: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
				
		END;  
		raise notice 'STEP14 % : % row(s) - V3 -END OF YEAR',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
		

	---------------------- EVENT 04 'advance premium'
 	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		--create table stag_a.stga_ifrs_nac_txn_step05  tablespace tbs_stag_a as 
		select  natrn.ref_1 as ref_1,adv.advance_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, adv.doc_dt
		, adv.doc_type 
		, natrn.doc_no
		, natrn.accode 
		, natrn.dc_flg , null::varchar as actype
		, (coalesce(adv.premium_amt,0) + coalesce(adv.extra_premium_amt,0))*-1 as posting_amt
		, null::varchar as system_type ,null::varchar as transaction_type,''::varchar  as premium_type 
		, coalesce(adv.policy_no,adv.temp_policy_no) as  policy_no ,adv.plan_cd as plan_cd , adv.rider_cd  as rider_cd , adv.pay_dt as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt  
		, adv.sales_id, adv.sales_struct_n, adv.selling_partner,adv.distribution_mode,adv.product_group,adv.product_sub_group,adv.rider_group,adv.product_term,adv.cost_center 
		, adv.advance_dim_txt 
		, 'dsmadvprem'::varchar(100) as source_filename , 'natrn-dsmadvprem'::varchar(100) as  group_nm  , 'advance premium'::varchar(100) as subgroup_nm , 'เบี้ยรับล่วงหน้า'::varchar(100)  as subgroup_desc   
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail ,natrn.org_branch_cd
		, ''::varchar(100) as org_submit_no , ''::varchar(100) as submit_no , ''::varchar(100) as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
		from  dds.ifrs_advance_payment_chg adv  
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn
		on (  natrn.doc_dt = adv.doc_dt
		and natrn.doc_type =  adv.doc_type
		and natrn.doc_no = adv.doc_no   
		and natrn.accode = adv.accode 
		and natrn.natrn_dim_txt = adv.advance_dim_txt 
		) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( adv.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( coalesce(adv.policy_no,adv.temp_policy_no)= pol.policy_no
		 and  adv.plan_cd = pol.plan_cd
		 and adv.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V4' 
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP15 V4 ADVANCE PREMIUM: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP15 % : % row(s) - V4 ADVANCE PREMIUM',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	---------------------- EVENT 05 
	--------------------- 05 Cut-off Advance payment (ตัดเบี้ยรับล่วงหน้า)
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium
		select  natrn.ref_1 as ref_1,adv.cofadvance_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, natrn.branch_cd as branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, adv.doc_dt
		, adv.doc_type 
		, natrn.doc_no
		, natrn.accode 
		, natrn.dc_flg , null::varchar as actype
		, coalesce(adv.premium_amt,0) + coalesce(adv.extra_premium_amt,0) as posting_amt
		, null::varchar as system_type ,null::varchar as transaction_type,''::varchar  as premium_type 
		, coalesce(adv.policy_no,adv.temp_policy_no) as  policy_no,trim(adv.plan_cd) as plan_cd 
		, adv.rider_cd  as rider_cd , adv.pay_dt as pay_dt ,''::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt  
		, adv.sales_id, adv.sales_struct_n, adv.selling_partner,adv.distribution_mode,adv.product_group,adv.product_sub_group,adv.rider_group,adv.product_term,adv.cost_center  
		, adv.cof_advance_dim_txt
		, 'cofadvprem' as source_filename , 'natrn-cofadvprem' as  group_nm  , 'cut off advance premium' as subgroup_nm , 'ตัดเบี้ยรับล่วงหน้า'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,natrn.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
		from dds.ifrs_cofadvance_payment_chg adv  
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn
		on (  natrn.doc_dt = adv.doc_dt
		and natrn.doc_type = adv.doc_type
		and natrn.doc_no = adv.doc_no   
		and natrn.accode = adv.accode 
		and natrn.natrn_dim_txt = adv.cof_advance_dim_txt ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( trim(adv.plan_cd) = trim(pp.plan_cd)) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( coalesce(adv.policy_no,adv.temp_policy_no)= pol.policy_no
		 and  adv.plan_cd = pol.plan_cd
		 and adv.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V5' 
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP16 V5 CUTOFF ADVANCE PREMIUM: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP16 % : % row(s) - V5 CUTOFF ADVANCE PREMIUM',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 
	---------------------- EVENT 07
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select natrn.ref_1 as ref_1 ,refund.refund_trans_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, refund.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, refund.doc_dt , refund.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, refund.posting_refund_amt   as posting_amt
		, refund.system_type ,refund.transaction_type,refund.premium_type 
		, refund.policy_no as policy_no ,refund.plan_cd as plan_cd , refund.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,refund.sales_id,refund.sales_struct_n,refund.selling_partner,refund.distribution_mode,refund.product_group,refund.product_sub_group,refund.rider_group,refund.product_term,refund.cost_center
		,refund.refund_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-refund' as  group_nm  , 'Refund Premium' as subgroup_nm ,'คืนเบี้ยเป็นเงินสด'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,refund.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,refund.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		inner join stag_a.ifrs_refund_trans_chg_no_dup refund -- dds.ifrs_refund_trans_chg  refund  
		on ( natrn.branch_cd =  refund.branch_cd 
		and natrn.doc_dt = refund.doc_dt
		and natrn.doc_type =  refund.doc_type 
		and natrn.doc_no = refund.doc_no 
		and  natrn.dc_flg = refund.dc_flg
		and natrn.accode = refund.accode 
		and natrn.natrn_dim_txt = refund.refund_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( refund.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( refund.policy_no= pol.policy_no
		 and  refund.plan_cd = pol.plan_cd
		 and refund.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
		--where  refund.transaction_type not in ( 'RID','RRID') --Log.180
		where natrn.control_dt = v_control_start_dt ; 

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP17 V7 Refund Premium: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP17 % : % row(s) - V7 -Refund Premium',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
			
		 ---------------------- EVENT 07
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1,account_acpaylink_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, acpay.doc_dt,acpay.doc_type,acpay.doc_no
		, acpay.accode,acpay.dc_flg 
		, acpay.actype as actype 
		, acpay.acpaylink_posting_amt as posting_amt
		, acpay.system_type as system_type,acpay.trans_type_cd as transaction_type,''::varchar as premium_type 
		, acpay.policy_no ,acpay.plan_cd, acpay.rider_type_cd as rider_cd ,null::date as pay_dt ,acpay.method_type as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt  
		,acpay.sales_id,acpay.sales_struct_n,acpay.selling_partner,acpay.distribution_mode,acpay.product_group,acpay.product_sub_group,acpay.rider_group,acpay.product_term,acpay.cost_center  
		,acpay.acpaylink_dim_txt 
		, 'acpaylink' as source_filename , 'natrn-acpaylink' as  group_nm  , 'Refund Premium ' as subgroup_nm , 'ระบบบันทึกบัญชีงานบริการ ข้อมูล Cancel Freelook'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		,natrn.detail as nadet_detail ,natrn.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,acpay.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from dds.ifrs_account_acpaylink_chg acpay   
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		on ( natrn.doc_dt = acpay.doc_dt
		and natrn.doc_type = acpay.doc_type
		and natrn.doc_no = acpay.doc_no 
		and natrn.dc_flg = acpay.dc_flg
		and natrn.accode = acpay.accode
		and natrn.natrn_dim_txt = acpay.acpaylink_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( acpay.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( acpay.policy_no= pol.policy_no
		 and  acpay.plan_cd = pol.plan_cd
		 and acpay.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and  vn.variable_cd = 'V7'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
		--WHERE acpay.trans_type_cd not in ( 'RID','RRID') --Log.180
		where natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP18 V7 Refund Premium Cert : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP18 % : % row(s) - V7 -Refund Premium Cert',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	---------------------- EVENT 07 - Payment Trans
	begin 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select natrn.ref_1 as ref_1 ,refund.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, refund.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, refund.doc_dt , refund.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, refund.posting_refund_amt   as posting_amt
		, refund.system_type ,refund.transaction_type,refund.premium_type 
		, refund.policy_no as policy_no ,coalesce (pol.plan_cd ,refund.plan_cd)  as plan_cd , refund.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,refund.sales_id,refund.sales_struct_n,refund.selling_partner,refund.distribution_mode,refund.product_group,refund.product_sub_group,refund.rider_group,refund.product_term,refund.cost_center
		,refund.transpos_dim_txt as refund_dim_txt  
		, 'payment_trans'::varchar as source_filename  , 'natrn-payment_trans' as  group_nm  , 'Refund' as subgroup_nm ,'คืนเบี้ยประกันปีต่อไป'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,refund.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,refund.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		inner join dds.ifrs_payment_transpos_chg  refund  
        on ( natrn.org_branch_cd =  refund.ref_branch_cd  --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
        and natrn.doc_dt = refund.doc_dt
        and natrn.doc_type =  refund.doc_type 
        and natrn.doc_no = refund.doc_no 
        and natrn.dc_flg = refund.dc_flg
        and natrn.accode = refund.accode 
        and natrn.natrn_dim_txt = refund.transpos_dim_txt  ) 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		 left outer join dds.tl_acc_policy_chg pol  
		 on (  refund.policy_no = pol.policy_no 
		 and refund.plan_cd = pol.plan_cd -- Oil 07Mar2023 : เพิ่ม join plan_cd Log340 อาจกระทบ log.199 
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
		--where refund.transaction_type not in ( 'RID','RRID') --Log.180
		where natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP19 V7 payment_trans : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP19 % : % row(s) - V7 -payment_trans',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
								 
		 ---------------------- EVENT 07 - Refund nac
	BEGIN
		insert into  stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount   as posting_amt -- posting_sum_nac_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename , 'natrn-nac' as  group_nm  , 'Refund Premium Not RID,RRID' as subgroup_nm , 'คืนเบี้ยเป็นเงินสด'  as subgroup_desc
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		inner join dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no= pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		--where dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true 
		where case when replace(nac.submit_no,' ','') = '' then true else  dds.fn_is_numeric(coalesce(trim(replace(nac.submit_no,' ','')),'0')) end 
		/* replace white space and enter */
		and  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium c 
							where nac.nac_rk = c.nac_rk 
							and c.group_nm  = 'natrn-nac' ) 
		and  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium cc -- log.174 Deduplicate with dds.ifrs_refund_chg
							where nac.branch_cd = cc.branch_cd 
							and nac.doc_dt = cc.doc_dt 
							and nac.doc_type = cc.doc_type 
							and nac.doc_no = cc.doc_no 
							and nac.accode = cc.accode 
							and nac.dc_flg = cc.dc_flg  
							and cc.group_nm  = 'natrn-refund' )
		and nac.transaction_type not in ( 'RID','RRID') --Log.180	 					
		and natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP20 V7 Refund Premium NAC : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP20 % : % row(s) - V7 -Refund Premium NAC',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	---------------------- EVENT 07 - Refund nac
	BEGIN
		insert into  stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount   as posting_amt -- posting_sum_nac_amt
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt 
		,nac.filename as source_filename , 'natrn-nac' as  group_nm  , 'Refund Premium RID,RRID' as subgroup_nm , 'คืนเบี้ยเป็นเงินสด'  as subgroup_desc
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem   as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  ) 
		inner join dds.oic_nac_chg nac 
		on (  natrn.branch_cd = nac.branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no= pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'  
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		--where dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true 
		where case when replace(nac.submit_no,' ','') = '' then true else  dds.fn_is_numeric(coalesce(trim(replace(nac.submit_no,' ','')),'0')) end 
		/* replace white space and enter */
		and  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium c 
							where nac.nac_rk = c.nac_rk 
							and c.group_nm  = 'natrn-nac' ) 
		and  not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium cc -- log.174 Deduplicate with dds.ifrs_refund_chg
							where nac.branch_cd = cc.branch_cd 
							and nac.doc_dt = cc.doc_dt 
							and nac.doc_type = cc.doc_type 
							and nac.doc_no = cc.doc_no 
							and nac.accode = cc.accode 
							and nac.dc_flg = cc.dc_flg  
							and cc.group_nm  = 'natrn-refund' )
		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
		and nac.transaction_type in ( 'RID','RRID')  				
		and natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP20 V7 Refund Premium NAC : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP20 % : % row(s) - V7 -Refund Premium NAC- RID RRID ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	--25Apr2022: Log.107 K.Ball CF ให้ย้ายจาก V8 ไปที่ V7
	--- EVENT 07 - Claim Refund
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk
		,nac.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt  
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		,nac.filename as source_filename , 'nac-claim' as  group_nm  , 'Claim Refund' as subgroup_nm ,  'Claim Refund - HQ (คืนเบี้ยงานสินไหมที่ สนญ)' as subgroup_desc 
		,vn.variable_cd
		,vn.variable_name as variable_nm 
		,natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable ,null::date as log_dttm 
		from dds.oic_nac_chg nac 
		inner join stag_a.stga_ifrs_nac_txn_step04_prem  natrn
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join stag_s.ifrs_common_accode  acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no= pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium'
		and vn.variable_cd = 'V7' 
		 and natrn.event_type  = vn.event_type 
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.branch_cd = '000' --natrn.source_type = 'H'
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn where txn.nac_rk = nac.nac_rk   )
		and exists (  select 1 
		                from dds.oic_adth_chg adth 
		                 where adth.pay_type_cd = '2'  
		                 and adth.source_filename in ('mgadth', 'adthtran')
		                 and adth.branch_cd = nac.branch_cd  
		                 and adth.ref_doc_dt = nac.doc_dt  
		                 and adth.ref_doc_no = nac.doc_no 
		                 and adth.policy_no = nac.policy_no --Log.67
		                 )  
		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
		--and nac.transaction_type not in ( 'RID','RRID') --Log.180	 
		and natrn.control_dt = v_control_start_dt ;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP21 V7 Claim Refund - HQ (คืนเบี้ยงานสินไหมที่ สนญ): p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP21 % : % row(s) - V7 Claim Refund - HQ (คืนเบี้ยงานสินไหมที่ สนญ)',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	--Oil 2023-04-10 : add ifrs_payment_reject_chg Log.356
	--EVENT 07 - Payment_Reject
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select natrn.ref_1 as ref_1 ,reject.payment_reject_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
        , reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
        , reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
        , natrn.accode , natrn.dc_flg , null::varchar as actype
        --, reject.reject_amt 
        -- 29Oct2023 Narumol W.: Premium Log 394 
        , reject.reject_posting_amt as posting_amt
        , reject.system_type ,reject.transaction_type,null::varchar as premium_type 
        , reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd , null::date as pay_dt ,''::varchar as pay_by_channel
       -- ,null::varchar as pay_period
        ,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        , natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
        , reject.natrn_dim_txt as refund_dim_txt  
        , 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  , 'Reject Premium' as subgroup_nm ,'Reject กลับรายการโอนเงินไม่สำเร็จ'  as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm 
        , natrn.detail as nadet_detail 
        ,reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
        ,reject.for_branch_cd
        ,natrn.event_type 
        ,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
        from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
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
        on ( vn.event_type = 'Premium'
        and vn.variable_cd = 'V7'  
        and natrn.premium_gmm_flg = vn.premium_gmm_flg
        and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) ) 
 		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
        -- where  reject.transaction_type not  in ( 'RID','RRID')
        and natrn.control_dt = v_control_start_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP21 V7 Payment_reject: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP21 % : % row(s) - V7 Payment Reject',clock_timestamp()::varchar(19),v_affected_rows::varchar;		

	-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
	-- EVENT 07 - Paymentapi_Reverse
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select natrn.ref_1 as ref_1 ,reject.payment_api_reverse_rk ::bigint as nac_rk
		, natrn.natrn_x_dim_rk
        , reject.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
        , reject.doc_dt,reject.doc_type,reject.doc_no
        , natrn.accode , natrn.dc_flg , null::varchar as actype
        , reject.posting_payment_api_reverse_amt as posting_amt
        , reject.system_type ,reject.transaction_type,null::varchar as premium_type 
        , reject.policy_no as policy_no ,reject.plan_cd as plan_cd , reject.rider_cd as rider_cd 
        , null::date as pay_dt , ''::varchar as pay_by_channel
        , posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        , natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
        , reject.product_group ,reject.product_sub_group 
        , reject.rider_group ,reject.product_term ,reject.cost_center 
        , reject.reverse_dim_txt as refund_dim_txt  
        , 'paymentapi_reverse'::varchar as source_filename , 'natrn-paymentapi_reverse' as group_nm 
        , 'paymentapi_reverse Premium' as subgroup_nm ,'PAYMENT API Reverse กลับรายการโอนเงินไม่สำเร็จ' as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm 
        , natrn.detail as nadet_detail 
        , reject.branch_cd as org_branch_cd , ''::varchar as org_submit_no 
        , ''::varchar as submit_no , ''::varchar as section_order_no , 0 as is_section_order_no
        , reject.for_branch_cd 
        , natrn.event_type 
        , natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
        , pol.policy_type ,pol.effective_dt ,pol.issued_dt
        , pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        , pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
        from  stag_a.stga_ifrs_nac_txn_step04_prem natrn 
        inner join  dds.ifrs_payment_api_reverse_chg   reject  
        on ( natrn.branch_cd =  reject.branch_cd 
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
        on ( vn.event_type = 'Premium'
        and vn.variable_cd = 'V7'  
        and natrn.premium_gmm_flg = vn.premium_gmm_flg
        and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) ) 
        and natrn.control_dt = v_control_start_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP21 V7 Payment_api_reverse: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP21 % : % row(s) - V7 Payment api_reverse',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	-- 05Jun2024 Narumol W. : [ITRQ#67052429] Enhance Variable Premium Condition - V7 V9 V13
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium   
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk  
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  
		, 'Accruedac_current'  as subgroup_nm , 'ตั้งค้างจ่ายสินไหมมรณกรรม'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04_prem natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.dc_flg = accru.dc_flg 
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'
		 and natrn.event_type  = vn.event_type
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	 
		where accru.doc_dt between v_control_start_dt and v_control_end_dt; 

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP22 V7 Accrued Ac : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP22 % : % row(s) - V7 Accrued Ac ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- 05Jun2024 Narumol W. : [ITRQ#67052463]เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link (enhance)
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium    
		select  accru.ref_1 as ref_1  
		, accru.accrual_n1_rk as nac_rk
		, null::int as natrn_x_dim_rk  
		, accru.branch_cd ,null::varchar as sap_doc_type,accru.ref_1 reference_header
		, accru.doc_dt as post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, 0::int  as sum_natrn_amt ,0::int as posting_sap_amt,0::int as posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group
		, accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'dds.ifrs_accrual_n1_chg' as source_filename , 'accrual_n1' as  group_nm 
		, 'ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link'::varchar(100) as subgroup_nm , 'ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, ''::varchar as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,accru.for_branch as for_branch_cd
		,coa.event_type 
		,coa.premium_gmm_flg ,coa.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  dds.ifrs_accrual_n1_chg accru   
		left join  stag_s.ifrs_common_coa coa 
		on ( coa.event_type = 'Premium'
		and accru.accode = coa.accode  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( accru.plan_code = pp.plan_cd)
		left outer join dds.tl_acc_policy_chg pol  
		 on ( accru.policy_no = pol.policy_no
		 and  accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'
		 and coa.event_type  = vn.event_type
		 and coa.premium_gmm_flg  = vn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )	 
		where accru.doc_dt between v_control_start_dt and v_control_end_dt 
		and accru.system_type_cd  in ( '072','073','074')
		and coa.premium_gmm_flg in ('4','7');
   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP23 V7 Accrued Ac N1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP23 % : % row(s) - V7 Accrued Ac N1 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	---------------------- EVENT 08
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , nac.nac_rk , coalesce(nac.natrn_x_dim_rk,natrn.natrn_x_dim_rk) as natrn_x_dim_rk 
		,nac.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,nac.doc_dt,nac.doc_type,nac.doc_no,nac.accode,nac.dc_flg,nac.actype 
		,nac.posting_amount as posting_amt 
		,nac.system_type,nac.transaction_type,nac.premium_type
		,nac.policy_no,nac.plan_cd, nac.rider_cd  ,nac.pay_dt ,nac.pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,nac.sales_id,nac.sales_struct_n,nac.selling_partner,nac.distribution_mode,nac.product_group,nac.product_sub_group,nac.rider_group,nac.product_term,nac.cost_center
		,nac.nac_dim_txt  
		, nac.filename as source_filename  , 'natrn-claim' as  group_nm  , 'Return premium - claim' as subgroup_nm , 'คืนเบี้ยจากการที่สินไหมปฏิเสธ'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd, nac.submit_no as org_submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(nac.submit_no) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup,vn.is_duplicate_variable   
		from  stag_a.stga_ifrs_nac_txn_step04_prem  as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.oic_nac_chg nac 
		on (  natrn.org_branch_cd = nac.ref_branch_cd --Log.204 Oil 19Sep2022 : change condition join from branch_cd to ref_branch_cd 
		and natrn.doc_dt = nac.doc_dt 
		and natrn.doc_type = nac.doc_type
		and natrn.doc_no = nac.doc_no 
		and natrn.accode = nac.accode 
		and natrn.dc_flg = nac.dc_flg
		and natrn.natrn_dim_txt = nac.nac_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( nac.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( nac.policy_no= pol.policy_no
		 and  nac.plan_cd = pol.plan_cd
		 and nac.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'  -- 24Aug2022 Narumol W.: Track_chg no.32 ย้าย V8 ไป V7
		 and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where  case when  
					case when replace(nac.submit_no,' ','') = '' then true else  dds.fn_is_numeric(coalesce(trim(replace(nac.submit_no,' ','')),'0')) end  is false 
				and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end = 1 
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn where txn.nac_rk = nac.nac_rk   )
		and natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP22 V8 Refund Premium CLAIM : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP22 % : % row(s) - V8 -Refund Premium CLAIM',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

  	---------------------- EVENT 09
	BEGIN	 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 ,unw.unwtoacc_rk::bigint as nac_rk ,  natrn.natrn_x_dim_rk  as natrn_x_dim_rk  
		,unw.branch_cd  ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,unw.doc_dt,unw.doc_type,unw.doc_no,unw.accode,'C' as dc_flg --Log.167
		,''::varchar as actype 
		,unw.posting_unwtoacc_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,unw.policy_no,unw.plan_cd,unw.rider_cd as rider_cd ,null::date as pay_dt ,null::varchar as pay_by_channel 
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,unw.sales_id,unw.sales_struct_n,unw.selling_partner,unw.distribution_mode,unw.product_group,unw.product_sub_group,unw.rider_group,unw.product_term,unw.cost_center
		,unw.unwtoacc_dim_txt as suspend_dim_txt   
		, unw.source_filename as source_filename  , 'natrn-suspense' as  group_nm  , 'Suspense new case' as subgroup_nm , 'รับรู้ Premium จากการล้าง suspense new case'  as subgroup_desc  
		,vn.variable_cd
		,vn.variable_name as variable_nm 
		,natrn.detail as nadet_detail 
		,unw.branch_cd as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no 
		,unw.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from stag_a.stga_ifrs_nac_txn_step04_prem natrn
		inner join dds.oic_unwtoacc_chg unw  
		on (  natrn.branch_cd = unw.branch_cd
		and natrn.doc_dt = unw.doc_dt
		and natrn.doc_type =  unw.doc_type 
		and natrn.doc_no =  unw.doc_no
		and natrn.accode = unw.accode 
		and natrn.natrn_dim_txt = unw.unwtoacc_dim_txt ) 
		left join stag_s.ifrs_common_accode  acc 
		on ( natrn.accode = acc.account_cd  )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( unw.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( unw.policy_no= pol.policy_no
		 and  unw.plan_cd = pol.plan_cd
		 and unw.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium'
		and vn.variable_cd = 'V9'  
		and natrn.premium_gmm_flg = vn.premium_gmm_flg
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where unw.approve_type_cd = '1'  and unw.account_flg = 'Y'
		and natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP23 V9 SUSPENSE NEW CASE - unwtoacc : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP23 % : % row(s) - V9 - SUSPENSE NEW CASE - unwtoacc',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
	---------------------- EVENT 09 จากการล้าง suspense new case
	/*
	11May2022 >> K.Ball Change condition to
	>> ข้อมูล Current month ส่งเฉพาะ ขาตั้ง
	>> ในแต่ละเดือนให้ส่งขาตั้งของ เดือน ธ.ค. แบบกลับ Sign
	*/
	begin
		  -- 05Jun2024 Narumol W. : [ITRQ#67052429] Enhance Variable Premium Condition - V7 V9 V13
			insert into stag_a.stga_ifrs_nac_txn_step05_premium 
			select  natrn.ref_1 as ref_1 ,sus.suspense_newcase_rk::bigint as nac_rk ,  natrn.natrn_x_dim_rk as natrn_x_dim_rk  
			,natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
			,sus.doc_dt,natrn.doc_type,sus.doc_no,natrn.accode,natrn.dc_flg,''::varchar as actype  
			,sus.posting_suspense_amt as posting_amt  
			,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
			,case when sus.policy_no = '00000000' then sus.x_policy_no else sus.policy_no end as policy_no
			,sus.plan_cd,sus.rider_cd as rider_cd ,sus.pay_dt ,null::varchar as pay_by_channel 
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,sus.sales_id,sus.sales_struct_n,sus.selling_partner,sus.distribution_mode,sus.product_group,sus.product_sub_group,sus.rider_group,sus.product_term,sus.cost_center
			,suspense_dim_txt
			, sus.filename as source_filename  , 'natrn-suspense' as  group_nm  
			, 'Accruedac_current' as subgroup_nm 
			, 'รับรู้ Premium จากการล้าง suspense new case'  as subgroup_desc  
			, vn.variable_cd
			, vn.variable_name as variable_nm 
			, natrn.detail as nadet_detail 
			, sus.branch_cd as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no 
			, sus.is_cut_off::int as is_section_order_no /* Use is_cutoff instead of is_section_no only in this case */
			,sus.for_branch_cd
			,natrn.event_type 
			,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			from  stag_a.stga_ifrs_nac_txn_step04_prem  as natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )
			inner join dds.ifrs_suspense_newcase_chg sus  
			on (  natrn.for_branch_cd = sus.for_branch_cd
			and natrn.doc_dt = sus.doc_dt  
			and natrn.doc_type = sus.doc_type
			and natrn.doc_no = sus.doc_no 
			and natrn.accode  = sus.c_accode 
			and natrn.natrn_dim_txt = sus.suspense_dim_txt   
			)
			left join  stag_s.stg_tb_core_planspec pp 
			on ( sus.plan_cd = pp.plan_cd) 
			left outer join dds.tl_acc_policy_chg pol  
		 	on ( sus.x_policy_no = pol.policy_no
			and sus.plan_cd = pol.plan_cd 
		 	and sus.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			inner join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Premium'
			and vn.variable_cd = 'V9'  
			and natrn.premium_gmm_flg = vn.premium_gmm_flg
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
			where  sus.is_cut_off ='0'
			and right('00'||date_part('month',sus.end_month_dt  ),2)::varchar(2) = '12' 
			and  coalesce( sus.mst_effective_dt ,sus.end_month_dt ) <= sus.end_month_dt   
			and natrn.control_dt = v_control_start_dt 
			union all 
			select  natrn.ref_1 as ref_1 ,sus.suspense_newcase_rk::bigint as nac_rk ,  natrn.natrn_x_dim_rk as natrn_x_dim_rk  
			,natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
			,sus.doc_dt,natrn.doc_type,sus.doc_no,natrn.accode,natrn.dc_flg,''::varchar as actype  
			,sus.posting_suspense_amt as posting_amt  
			,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
			,case when sus.policy_no = '00000000' then sus.x_policy_no else sus.policy_no end as policy_no
			,sus.plan_cd,sus.rider_cd as rider_cd ,sus.pay_dt ,null::varchar as pay_by_channel 
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,sus.sales_id,sus.sales_struct_n,sus.selling_partner,sus.distribution_mode,sus.product_group,sus.product_sub_group,sus.rider_group,sus.product_term,sus.cost_center
			,suspense_dim_txt
			, sus.filename as source_filename  , 'natrn-suspense' as  group_nm  , 'Accruedac_current' as subgroup_nm , 'รับรู้ Premium จากการล้าง suspense new case'  as subgroup_desc  
			, vn.variable_cd
			, vn.variable_name as variable_nm 
			, natrn.detail as nadet_detail 
			, sus.branch_cd as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no 
			, sus.is_cut_off::int as is_section_order_no /* Use is_cutoff instead of is_section_no only in this case */
			,sus.for_branch_cd
			,natrn.event_type 
			,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			from  stag_a.stga_ifrs_nac_txn_step04_prem  as natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )
			inner join dds.ifrs_suspense_newcase_chg sus   
			on (  natrn.for_branch_cd = sus.for_branch_cd
			and natrn.doc_dt = sus.doc_dt  
			and natrn.doc_type = sus.doc_type
			and natrn.doc_no = sus.doc_no 
			and natrn.accode  = sus.c_accode 
			and natrn.natrn_dim_txt = sus.suspense_dim_txt  
			-- and natrn.sales_id  = sus.sales_id 
			)
			left join  stag_s.stg_tb_core_planspec pp 
			on ( sus.plan_cd = pp.plan_cd)
			left outer join dds.tl_acc_policy_chg pol  
		 	on ( sus.x_policy_no = pol.policy_no
			and sus.plan_cd = pol.plan_cd 
		 	and sus.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			inner join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Premium'
			and vn.variable_cd = 'V9'  
			and natrn.premium_gmm_flg = vn.premium_gmm_flg
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
			where sus.is_cut_off ='0'
			and right('00'||date_part('month',sus.end_month_dt ),2)::varchar(2) <> '12' 
			and  coalesce( sus.mst_effective_dt ,sus.end_month_dt ) <= sus.end_month_dt  
			and coalesce( sus.pay_dt,sus.end_month_dt ) <= sus.end_month_dt 
			and coalesce( sus.account_dt,sus.end_month_dt )  <= sus.end_month_dt 
			and coalesce( sus.nb_effective_dt,sus.end_month_dt ) <= sus.end_month_dt 
			and coalesce( sus.effective_dt,sus.end_month_dt ) <= sus.end_month_dt 
			and coalesce( sus.policy_dt,sus.end_month_dt ) <= sus.end_month_dt 
			and natrn.control_dt = v_control_start_dt ;  
		
			-- ** ในเดือน ม.ค. ของทุกปึให้ส่งขาตั้งของ เดือน ธ.ค. ปีที่แล้ว แบบกลับ Sign
			-- 05Jun2024 Narumol W. : [ITRQ#67052429] Enhance Variable Premium Condition - V7 V9 V13
			-- 10Jun2025 Nuttadet O.: [ITRQ#68062184] Premium Variable - แก้ไขชื่อ table ใน V9 เนื่องจากมีการเปลี่ยน source '%newcase%' เป็น 'gl_transaction.newcase%'
			
			if date_part('month',v_control_start_dt) = 1  then -- Oil 06Jun2022 : Track_chg 67
				insert into stag_a.stga_ifrs_nac_txn_step05_premium 
				select ref_1,nac_rk,natrn_x_dim_rk,branch_cd,sap_doc_type,reference_header
				,v_control_end_dt as post_dt
				,doc_dt,doc_type,doc_no,accode,dc_flg,actype
				,posting_amt*-1 as posting_amt
				,system_type,transaction_type,premium_type
				,policy_no,plan_cd,rider_cd,pay_dt,pay_by_channel,sum_natrn_amt,posting_sap_amt,posting_proxy_amt
				,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group,product_sub_group
				,rider_group,product_term,cost_center,nac_dim_txt
				,source_filename,group_nm
				-- Change Accruedac_current to 'Accrued Ac'
				, 'Accrued Ac' as subgroup_nm 
				,subgroup_desc,variable_cd,variable_nm
				,nadet_detail,org_branch_cd,org_submit_no,submit_no,section_order_no
				,is_section_order_no,for_branch_cd,event_type,premium_gmm_flg
				,premium_gmm_desc,policy_type,effective_dt,issued_dt,ifrs17_channelcode
				,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup
				,is_duplicate_variable 
				from dds.ifrs_variable_premium_ytd  ivpy 
				where variable_cd = 'V9'
				and source_filename like 'gl_transaction.newcase%'
				and date_part('year',post_dt ) =  date_part('year',v_control_end_dt) -1
				and date_part('month',post_dt ) = 12 
				and date_part('year',doc_dt ) = date_part('year',v_control_end_dt) -1 
				and is_section_order_no  = 0  ; 
			
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			end if;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP24 V9 SUSPENSE NEW CASE : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP24 % : % row(s) - V9 - SUSPENSE NEW CASE',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 
	-- from LogNo.59 - ยกเลิก , Log 106 เอากลับมา
		 ---------------------- EVENT 01 - NATRN-CLAIM 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , adth.adth_rk  , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt  
		,adth.ref_doc_dt doc_dt,adth.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.posting_claim_amt as posting_claim_amt 
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd --,adth.pay_dt as  pay_dt ,adth.method_type_cd as pay_by_channel
		,null::date as  pay_dt ,null::varchar as pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as adth_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'claim paid' as subgroup_nm ,  'เบี้ยประกันปีต่อไป' as subgroup_desc  
 		, vn.variable_cd
		, vn.variable_name as variable_nm   
		, natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.premium_gmm_flg,natrn.premium_gmm_desc
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem  natrn
		 inner join dds.oic_adth_chg  adth   --23Nov22 NarumolW. : change source from ifrs_adth_pay_chg to oic_adth_chg
		 on (   adth.branch_cd = natrn.branch_cd  
			and adth.ref_doc_dt = natrn.doc_dt  
		 	and adth.ref_doc_no = natrn.doc_no
			and adth.accode = natrn.accode
            and adth.adth_dim_txt = natrn.natrn_dim_txt) ---log 175 
		 left join stag_s.ifrs_common_accode  acc 
		 on ( adth.accode = acc.account_cd  ) 
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V1'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 ) 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( adth.policy_no = pol.policy_no 
		 and adth.plan_cd = pol.plan_cd
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 where adth.pay_type_cd = '2'
		 and adth.branch_cd = '000'  
		 and adth.source_filename in ('mgadth', 'adthtran')
		 and not exists   ( select 1 from dds.oic_nac_chg nac 
		 							where  adth.branch_cd = nac.branch_cd  
									and adth.ref_doc_dt = nac.doc_dt  
								 	and adth.ref_doc_no = nac.doc_no
								 	and adth.section_order_no = nac.submit_no
									and adth.policy_no = nac.policy_no)	  
		and natrn.control_dt = v_control_start_dt ;
 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP25 V1 CLAIM POS : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP25 % : % row(s) - V1 CLAIM POS',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
 
		 ---------------------- EVENT 07 - NATRN-CLAIM 
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select  natrn.ref_1 as ref_1 , adth.adth_rk  , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt  
		,adth.ref_doc_dt doc_dt,adth.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.posting_claim_amt as posting_claim_amt 
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd ,null::date  pay_dt ,null::varchar pay_by_channel -- Log.220
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as adth_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'claim refund' as subgroup_nm ,  'คืนเบี้ยประกันปีต่อไป' as subgroup_desc  
 		, vn.variable_cd
		, vn.variable_name as variable_nm   
		, natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.premium_gmm_flg,natrn.premium_gmm_desc   
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem  natrn
		 inner join dds.oic_adth_chg  adth --Oil 13Sep2022 : Cheng source from  dds.ifrs_adth_pay_chg to dds.oic_adth_chg  Log.220    
		 on (   adth.branch_cd = natrn.branch_cd  
			and adth.ref_doc_dt = natrn.doc_dt  
		 	and adth.ref_doc_no = natrn.doc_no  
		 	and adth.accode = natrn.accode
		 	and adth.adth_dim_txt = natrn.natrn_dim_txt) ---log 175 
		 left join stag_s.ifrs_common_accode  acc 
		 on ( adth.accode = acc.account_cd  ) 
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Premium'
		 and vn.variable_cd = 'V7'
		 and vn.premium_gmm_flg  = natrn.premium_gmm_flg
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future )
		 )
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( adth.policy_no = pol.policy_no 
		 and adth.plan_cd = pol.plan_cd
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 where adth.pay_type_cd = '2'
		 and not exists   ( select 1 from dds.oic_nac_chg nac 
		 							where  adth.branch_cd = nac.branch_cd  
									and adth.ref_doc_dt = nac.doc_dt  
								 	and adth.ref_doc_no = nac.doc_no
								 	and adth.section_order_no = nac.submit_no)	 
		and natrn.control_dt = v_control_start_dt ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP26 V7 Refund CLAIM POS : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP26 % : % row(s) - V7 Refund CLAIM POS',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

--======================== V1 - Premium  บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน ==============================--               
	BEGIN       
		
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		 select  natrn.ref_1 as ref_1 , adth.adth_fax_claim_rk as adth_rk  , natrn.natrn_x_dim_rk   
        ,adth.branch_cd 
        ,natrn.sap_doc_type,natrn.reference_header::varchar(100) ,natrn.post_dt         
        ,adth.doc_dt,natrn.doc_type,adth.doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
        ,adth.posting_claim_amt as posting_amt 
        ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
        ,adth.policy_no,adth.plan_cd, adth.rider_cd  
        ,null::date as  pay_dt ,null::varchar as pay_by_channel
        ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
        ,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
        ,adthtran_dim_txt as nac_dim_txt  
        ,adth.source_filename::varchar(100) , 'natrn-fax claim'::varchar(100) as  group_nm  
        , 'Premium'::varchar(100)  as subgroup_nm 
        , 'บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน'::varchar(100)  as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm  
        , natrn.detail as nadet_detail 
        , adth.org_branch_cd,null::varchar as org_submit_no
        , null::varchar(20) as submit_no
        , adth.section_order_no
        , 0::int as is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type  
        ,natrn.premium_gmm_flg,natrn.premium_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup
        ,vn.is_duplicate_variable ,null::date as log_dttm   
        from  stag_a.stga_ifrs_nac_txn_step04_prem  natrn
         inner join dds.ifrs_adth_payment_api_chg adth  
         on (   adth.branch_cd = natrn.branch_cd  
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
         on ( vn.event_type = 'Premium'
         and vn.variable_cd = 'V1'
         and vn.premium_gmm_flg = natrn.premium_gmm_flg 
         )
        left outer join dds.tl_acc_policy_chg pol  
         on (  adth.policy_no = pol.policy_no
         and  adth.plan_cd = pol.plan_cd
         and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
         where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium bb where bb.nac_rk = adth.adth_fax_claim_rk  )
		 and natrn.premium_gmm_flg not in ('4')               
         and natrn.control_dt = v_control_start_dt ;
   		
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP27 V1 บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP27 % : % row(s) - V1 บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
    

--======================== V7 - Return-Premium บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน ==============================--
	BEGIN	     

        insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , adth.adth_fax_claim_rk as adth_rk  , natrn.natrn_x_dim_rk   
        ,adth.branch_cd 
        ,natrn.sap_doc_type,natrn.reference_header::varchar(100) ,natrn.post_dt         
        ,adth.doc_dt,natrn.doc_type,adth.doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
        ,adth.posting_claim_amt as posting_amt--, adth.claim_amt  as claim_amt 
       -- ,adth.posting_claim_amt as posting_claim_pay_amt, adth.claim_amt  as claim_pay_amt  
        ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
        ,adth.policy_no,adth.plan_cd, adth.rider_cd  
        ,null::date as  pay_dt ,null::varchar as pay_by_channel
        ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
        ,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
        ,adthtran_dim_txt as nac_dim_txt  
        ,adth.source_filename::varchar(100) , 'natrn-fax claim'::varchar(100) as  group_nm  
        , 'Premium'::varchar(100)  as subgroup_nm 
        , 'บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน'::varchar(100)  as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm  
        , natrn.detail as nadet_detail 
        , adth.org_branch_cd,null::varchar as org_submit_no
        , null::varchar(20) as submit_no
        , adth.section_order_no
        , 0::int as is_section_order_no
        ,natrn.for_branch_cd
        ,natrn.event_type  
        ,natrn.premium_gmm_flg,natrn.premium_gmm_desc 
        ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup
        ,vn.is_duplicate_variable ,null::date as log_dttm 
        from  stag_a.stga_ifrs_nac_txn_step04_prem  natrn
         inner join dds.ifrs_adth_payment_api_chg adth  
         on (   adth.branch_cd = natrn.branch_cd  
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
         on ( vn.event_type = 'Premium'
         and vn.variable_cd = 'V7'
         and vn.premium_gmm_flg = natrn.premium_gmm_flg 
         )
        left outer join dds.tl_acc_policy_chg pol  
         on (  adth.policy_no = pol.policy_no
         and  adth.plan_cd = pol.plan_cd
         and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
         where not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium bb where bb.nac_rk = adth.adth_fax_claim_rk  )
		 and natrn.premium_gmm_flg in ('4')               
         and natrn.control_dt = v_control_start_dt  ;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP28 V7 - Return-Premium  บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP28 % : % row(s) - V7 - Return-Premium  บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	--- ===================================================== V13 sap_trial_balance 
	--Oil 20230516 : Add V13 stag_f.tb_sap_trial_balance >> ปรับใช้ dds.tb_sap_trial_balance
	--19Oct2023 Narumol W. : [ITRQ#66104655] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
	-- 05Jun2024 Narumol W. : [ITRQ#67052429] Enhance Variable Premium Condition - V7 V9 V13 ยกเลิก V13
	/*
	BEGIN			
			
 		insert into stag_a.stga_ifrs_nac_txn_step05_premium
	    ( post_dt, doc_dt,accode,posting_amt
	    ,event_type,source_filename,group_nm,subgroup_nm,subgroup_desc
	    ,variable_cd,variable_nm
	    , premium_gmm_flg,premium_gmm_desc  ) 
	    select report_date as post_dt,report_date as doc_dt
		, trn.gl_acct as accode 
		, trn.accumulated_balance*-1 as posting_amt
		, 'Premium' as event_type 
		, 'sap_trial_balance' as source_filename
		, 'SAP_TB' as group_nm 
		, 'Accruedac_current' as subgroup_nm 
		, 'V13-ยอดคงเหลือพักเบี้ยเคสใหม่' as subgroup_desc
		, coa.dummy_variable_nm as variable_cd   
		, coalesce(vn.variable_name, coa.variable_for_policy_missing)  as variable_nm
		,coa.premium_gmm_flg ,coa.premium_gmm_desc  
		from  dds.tb_sap_trial_balance trn 
		 inner join stag_s.ifrs_common_coa  coa
		on ( coa.event_type = 'TB_Premium'
		and coa.accode  = trn.gl_acct  ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'TB_Premium'
        and vn.variable_cd = 'V13' 
        and coa.premium_gmm_flg  = vn.premium_gmm_flg ) 
        where trn.ledger = '0l'
        and report_date = v_control_end_dt;
			                
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP29 V13 - SAP_TB sap_trial_balance : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP29 % : % row(s) - V13 - SAP_TB sap_trial_balance',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	*/
		-----------------------------------------------------------  
	BEGIN	
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
		
		------------- Manual  
		-- Oil 11Nov2022 : Trach change 47
		-- STEP_01 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) <= 8		
		-- 25Oct2023 Narumol W. : Add step join with policy and plan_cd after patch policy & plan_Cd 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no,coalesce(pol.plan_cd,natrn.plan_cd) as plan_cd 
		,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,natrn.natrn_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )   
		inner join dds.tl_acc_policy_chg pol  
		 on (  natrn.nadet_policyno  = pol.policy_no
		 and natrn.plan_cd  = pol.plan_cd
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and vn.variable_cd = 'V1'   
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Premium'
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and natrn.control_dt = v_control_start_dt ;  
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;	 
 		raise notice 'STEP30.1 % : % row(s) - V1 - join with policy and plan_cd after patch policy & plan_Cd ',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 	
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no,coalesce(pol.plan_cd,natrn.plan_cd) as plan_cd 
		,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,natrn.natrn_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )   
		left outer join dds.tl_acc_policy_chg pol  
		 on (  natrn.nadet_policyno  = pol.policy_no
		 and pol.policy_type not in ( 'M','G')  --Log.143
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and vn.variable_cd = 'V1'   
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Premium'
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and natrn.control_dt = v_control_start_dt  
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium c 
  		 					where natrn.branch_cd = c.branch_cd
  		 					and natrn.doc_dt = c.doc_dt  
  		 					and natrn.doc_type = c.doc_type  
  		 					and natrn.doc_no = c.doc_no  
  		 					and natrn.accode = c.accode
  		 					and natrn.nadet_policyno = c.policy_no
  		 					);
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;	 
 		raise notice 'STEP30.2 % : % row(s) - V1 - Manual DOCNO 5 STEP_01 length nadet_policyno <= 8',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 
		-- STEP_02 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) > 9 
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		-- 02Oct2023 Narumol W. : Patch policy from nadet
		,pol.policy_no as policy_no,coalesce(pol.plan_cd,natrn.plan_cd) as plan_cd 
		,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,natrn.natrn_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )   
		left outer join dds.tl_acc_policy_chg pol  
		 on (  trim(natrn.nadet_policyno) = concat(trim(pol.plan_cd),trim(pol.policy_no))
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and vn.variable_cd = 'V1'   
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Premium'
		and length(nullif(trim(natrn.nadet_policyno),''))> 9 
		and natrn.control_dt = v_control_start_dt ;  
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP30.3 % : % row(s) - V1 - Manual DOCNO 5 STEP_02 length nadet_policyno >  9 ',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 	 

			------------- Manual  
		-- STEP_01 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		-- 25Oct2023 Narumol W. : Add step join with policy and plan_cd after patch policy & plan_Cd 
		
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,coalesce(pol.plan_cd,natrn.plan_cd) as plan_cd,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,natrn.natrn_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		inner join dds.tl_acc_policy_chg pol  
		 on (  natrn.nadet_policyno  = pol.policy_no
		 and natrn.plan_cd  = pol.plan_cd
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and vn.variable_cd = 'V7'   
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Premium'
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and natrn.control_dt = v_control_start_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP31.1 % : % row(s) - V7 -  join with policy and plan_cd after patch policy & plan_Cd ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,coalesce(pol.plan_cd,natrn.plan_cd) as plan_cd,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,natrn.natrn_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left outer join dds.tl_acc_policy_chg pol  
		 on (  natrn.nadet_policyno  = pol.policy_no
		 and pol.policy_type not in ( 'M','G')  --Log.143
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and vn.variable_cd = 'V7'   
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Premium'
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and natrn.control_dt = v_control_start_dt 
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_premium c 
  		 					where natrn.branch_cd = c.branch_cd
  		 					and natrn.doc_dt = c.doc_dt  
  		 					and natrn.doc_type = c.doc_type  
  		 					and natrn.doc_no = c.doc_no  
  		 					and natrn.accode = c.accode
  		 					and natrn.nadet_policyno = c.policy_no
  		 					);
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP31.2 % : % row(s) - V7 - Manual DOCNO 5 STEP_01 length nadet_policyno <= 8 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 
		-- STEP_02 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) > 9
	
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		-- 02Oct2023 Narumol W. : Patch policy from nadet
		,pol.policy_no as policy_no
		,coalesce(pol.plan_cd,natrn.plan_cd) as plan_cd,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,natrn.natrn_dim_txt 
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
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step04_prem natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left outer join dds.tl_acc_policy_chg pol  
		 on (  trim(natrn.nadet_policyno) = concat(trim(pol.plan_cd),trim(pol.policy_no))
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg
		and vn.variable_cd = 'V7'   
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.doc_no like '5%'
		and natrn.event_type  = 'Premium'
		and length(nullif(trim(natrn.nadet_policyno),''))> 9 
		and natrn.control_dt = v_control_start_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP31.3 % : % row(s) - V7 - Manual DOCNO 5 STEP_02 length nadet_policyno > 9 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP30 VX Manual DOCNO 5 STEP_02 length nadet_policyno > 9 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	--raise notice 'STEP20 % : % row(s) - VX - Manual DOCNO 5',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
		-------------- manual_pvyymm
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select sap.ref1_header as ref_1 ,null::bigint as nac_rk , null::bigint as natrn_x_dim_rk  
		,right(pv.process_branch,3) as branch_cd ,sap.doc_type as sap_doc_type,left( sap.ref1_header,6) as reference_header,sap.posting_date_dt
		,pv.doc_date doc_dt,null::varchar(3) as doc_type,null::varchar(6)  as doc_no,pv.sap_acc_code as accode, pk.dc_flg,''::varchar as actype 
		,pv.amt_doc_currency   as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,pv.ref_key_1 as policy_no,pol.plan_cd as plan_cd , pv.rider_type as rider_cd 
		,null::date as pay_dt , null::varchar as pay_by_channel
		,null::numeric as sum_natrn_amt ,posting_sap_amt,null::numeric as posting_proxy_amt  
		,null::varchar as sales_id,null::varchar as sales_struct_n
		,pv.selling_partner,pv.distribution_mode,pv.product_group,pv.product_sub_group,pv.rider_group,pv.product_term,pv.cost_center
		,concat(pv.selling_partner,pv.distribution_mode,pv.product_group,pv.product_sub_group,pv.rider_group,pv.product_term,pv.cost_center) pv_dim_txt
		,'manual_pvyymm' as source_filename  , 'sap-pv' as  group_nm  , 'Manual' as subgroup_nm , 'App Superman Key Manual'  as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		,null::varchar as nadet_detail
		,right(pv.process_branch,3) as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no 
		,null::varchar as for_branch_cd
		,coa.event_type 
		,coa.premium_gmm_flg ,coa.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step01_2 as sap 
		join stag_s.stg_tb_gl_transaction_manual_pvyymm as pv 
		on ( sap.company_code = pv.company_code 
		and sap.fiscal_year = date_part('year',pv.post_date)
		and sap.doc_type = pv.doc_type 
		and sap.posting_date_dt =  pv.post_date 
		and sap.account_no = pv.sap_acc_code
		and sap.sap_dim_txt = concat(pv.selling_partner,pv.distribution_mode,pv.product_group,pv.product_sub_group,pv.rider_group,pv.product_term,'00'||pv.cost_center)) 
		left outer join stag_s.stg_common_oic_posting_key pk 
	 	on ( pv.post_key = pk.posting_key )
	 	left outer join stag_s.ifrs_common_coa  coa  
		on (  pv.sap_acc_code = coa.accode  )	 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( pv.ref_key_1 = pol.policy_no 
		 and pv.doc_date between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and coa.premium_gmm_flg  = vn.premium_gmm_flg 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where coa.event_type  = 'Premium'
		and pv.doc_date between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP32 VX PV App Superman Key Manual: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP29 % : % row(s) - VX - PV App Superman Key Manual',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	 
	-------------- manual_jvyymm
	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  
		select sap.ref1_header as ref_1 ,null::bigint as nac_rk , null::bigint as natrn_x_dim_rk  
		,right(pv.process_branch,3) as branch_cd ,sap.doc_type as sap_doc_type,left( sap.ref1_header,6) as reference_header,sap.posting_date_dt
		,pv.doc_date doc_dt,null::varchar(3) as doc_type,null::varchar(6)  as doc_no,pv.sap_acc_code as accode, pk.dc_flg,''::varchar as actype 
		,pv.amt_doc_currency   as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,pv.ref_key_1 as policy_no, pol.plan_cd as plan_cd , pv.rider_type as rider_cd 
		,null::date as pay_dt , null::varchar as pay_by_channel
		,null::numeric as sum_natrn_amt ,posting_sap_amt,null::numeric as posting_proxy_amt  
		,null::varchar as sales_id,null::varchar as sales_struct_n
		,pv.selling_partner,pv.distribution_mode,pv.product_group,pv.product_sub_group,pv.rider_group,pv.product_term,pv.cost_center
		,concat(pv.selling_partner,pv.distribution_mode,pv.product_group,pv.product_sub_group,pv.rider_group,pv.product_term,pv.cost_center) pv_dim_txt
		,'manual_jvyymm' as source_filename  , 'sap-jv' as  group_nm  , 'Manual' as subgroup_nm , 'App Superman Key Manual'  as subgroup_desc 
		, vn.variable_cd, vn.variable_name as variable_nm  
		,null::varchar as nadet_detail
		,right(pv.process_branch,3) as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no 
		,null::varchar as for_branch_cd
		,coa.event_type 
		,coa.premium_gmm_flg ,coa.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_step01_2 as sap 
		right join stag_s.stg_tb_gl_transaction_manual_jvyymm as pv 
		on ( sap.company_code = pv.company_code 
		and sap.fiscal_year = date_part('year',pv.post_date)
		and sap.doc_type = pv.doc_type 
		and sap.posting_date_dt =  pv.post_date 
		and sap.account_no = pv.sap_acc_code
		and sap.sap_dim_txt = concat(pv.selling_partner,pv.distribution_mode,pv.product_group,pv.product_sub_group,pv.rider_group,pv.product_term,'00'||pv.cost_center)
		) 
		left outer join stag_s.stg_common_oic_posting_key pk 
	 	on ( pv.post_key = pk.posting_key )
	 	left outer join stag_s.ifrs_common_coa  coa 
		on (  pv.sap_acc_code = coa.accode  )	 
		left outer join dds.tl_acc_policy_chg pol  
		 on ( pv.ref_key_1 = pol.policy_no 
		 and pv.doc_date between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and coa.premium_gmm_flg  = vn.premium_gmm_flg 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where coa.event_type  = 'Premium' 
		and pv.doc_date between v_control_start_dt and v_control_end_dt;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP30 VX PV App Superman Key Manual: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP30 % : % row(s) - VX - PV App Superman Key Manual',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
  
	-- log.180 - Exclude ไม่เอารายการคืนเบี้ยของแบบประกัน Unit Linked ที่ transaction_type = "RID" or "RRID" เข้า mapping variable 
 	-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
	/*BEGIN
		delete 
		from stag_a.stga_ifrs_nac_txn_step05_premium
		where variable_cd = 'V7'
		and transaction_type  in ( 'RID','RRID');
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP30 VX PV App Superman Key Manual: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP31 % : % row(s) - Delete - Exclude RID,RRID',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
  	*/

 	BEGIN	
		-- Reconcile 
		--drop table if exists stag_a.stga_ifrs_nac_txn_missing_step05_premium;
		--create table stag_a.stga_ifrs_nac_txn_missing_step05_premium tablespace tbs_stag_a as 
		 
		truncate table stag_a.stga_ifrs_nac_txn_missing_step05_premium ;
		insert into stag_a.stga_ifrs_nac_txn_missing_step05_premium 
		select distinct ref_1,natrn_x_dim_rk,branch_cd,org_branch_cd,doc_dt,doc_type,doc_no,sap_doc_type,reference_header,post_dt
		,accode,dc_flg,natrn_amt,posting_natrn_amt,posting_sap_amt,posting_proxy_amt,detail,nadet_policyno,plan_cd,reverse_doc_no
		,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center
		,natrn_dim_txt,is_accru,is_accru_type,is_manual,for_branch_cd,event_type,premium_gmm_flg,premium_gmm_desc
		,claim_gmm_flg,claim_gmm_desc,benefit_gmm_flg,benefit_gmm_desc,groupeb_gmm_flg,groupeb_gmm_desc,comm_gmm_flg,comm_gmm_desc
		,filename,is_reverse,source_type,upd_dttm,unitlink_gmm_flg,unitlink_gmm_desc 
		from  (  
		select step4.* 
		from  stag_a.stga_ifrs_nac_txn_step04_prem step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_premium step5
		on ( step4.natrn_x_dim_rk = step5.natrn_x_dim_rk ) 
		where step5.ref_1 is null
		and step4.natrn_x_dim_rk is not null 
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt  
		--and step4.ref_1 not in ( 'ACAPRM20210131001','ACAPRM20210131002') 
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Premium')
		
		union  
		select step4.* 
		from  stag_a.stga_ifrs_nac_txn_step04_prem step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_premium step5
		on ( step4.ref_1 = step5.ref_1
		and step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no ) 
		where step5.ref_1 is null and step4.natrn_x_dim_rk is null 
		and  step4.doc_dt between  v_control_start_dt and v_control_end_dt  
		--and step4.ref_1 not in ( 'ACAPRM20210131001','ACAPRM20210131002') 
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Premium')
		) as aa ;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP31 Reconcile : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;	 

 	raise notice 'STEP31 % : % row(s) - Reconcile ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

-- =================================== MANUAL from GL SAP =========================== -- 
	BEGIN	
		------------- Manual  
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode , dc_flg ,   posting_amt 
		 ,policy_no ,plan_cd 
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		,source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		,variable_cd ,variable_nm, event_type ,premium_gmm_flg ,premium_gmm_desc
		,rider_cd)
 		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg  ,posting_sap_amt 
		--, case when left(account_no,1) in ( '4','5') and length(ref_key1) = 8 then ref_key1 end as policy_no --Log.257
		--, ref_key1 as policy_no
		--, coalesce(gl.ref_key1,gl.policy_no) as policy_no  --Log.257
		, coalesce(gl.policy_no,gl.ref_key1) as policy_no  -- 09Jan2024 Narumol W. : fix Log.416 
		, coalesce(gl.plan_cd,pol.plan_cd) as plan_cd
		, posting_sap_amt ,posting_sap_amt 
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		,gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		-- 29Jan2024 Narumol W. : [ITRQ#67010241] ขอเปลี่ยนชื่อ source_filename เป็น 'sap'
		,'sap' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT')as variable_cd --Log.177
        , coalesce (vn.variable_name ,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
		, coa.event_type , coa.premium_gmm_flg ,coa.premium_gmm_desc 
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)  
		from stag_a.stga_ifrs_nac_txn_step01 gl 
		inner join stag_s.ifrs_common_coa coa 
		on ( gl.account_no = coa.accode  )   
		left outer join dds.tl_acc_policy_chg pol  
		 on ( gl.policy_no  = pol.policy_no
         --Log.230 
		 --and pol.policy_type not in ( 'M','G')
		 and gl.plan_cd = pol.plan_cd 
		 and doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd) 
        left outer join stag_s.ifrs_common_variable_nm vn -- log.177
        on ( coa.event_type = vn.event_type 
        and coa.premium_gmm_flg  = vn.premium_gmm_flg
        and vn.variable_cd in ( 'V1','V7') 
        and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) ) 
        where coa.event_type  = 'Premium' 
		and doc_type not in ( 'SI','SB')
		and coalesce(coalesce(gl.plan_cd,pol.plan_cd),'') not in ('GT10','GT11')-- Log.252 ,266
		--and accode not in ('4011020040','4011020043') --log.178
		--07Sept2022 Oil : update นำรายการที่เป็นของ M907 ออก Log.178 
		and posting_date_dt  between v_control_start_dt and v_control_end_dt
		--07Jul2023 Narumol W. : ITRQ-66073307: เพิ่มเงื่อนไข V2,3 PREMIUM_YTD เบี้ยค้างรับจาก TLP เก่า : Exclude from manual_gl_proxy
		and not ( coalesce(gl.doc_type,'') in ( 'SA' ,'SX' )
				and gl.plan_cd  = '_TLP'
				and coalesce(gl.assignment_code,'') ='A');
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP34 VX Manual PROXY : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP34 % : % row(s) - VX - Manual PROXY ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	
 
 	begin -- select * from stag_a.stga_ifrs_nac_txn_step05_premium
	 	
	 	/*
		------------- DUMMY  
		insert into stag_a.stga_ifrs_nac_txn_step05_premium  	
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt 
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no,natrn.plan_cd,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
			,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename  
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, coalesce (vn.variable_cd,coa.dummy_variable_nm) as variable_cd
		, coalesce (vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from  stag_a.stga_ifrs_nac_txn_missing_step05_premium natrn -- select premium_gmm_flg from stag_a.stga_ifrs_nac_txn_step04 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd)
		left outer join stag_a.stga_tl_acc_policy pol  
		on ( natrn.nadet_policyno  = pol.policy_no
		and  natrn.plan_cd= pol.plan_cd )
		left outer join stag_s.ifrs_common_coa coa 
		on ( natrn.accode = coa.accode  )  
		left join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Premium' 
		and natrn.premium_gmm_flg  = vn.premium_gmm_flg 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
		where natrn.event_type  = 'Premium' 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
	*/
	
		------------- DUMMY  
		insert into stag_a.stga_ifrs_nac_txn_step05_premium 
		select ref_1,nac_rk,natrn_x_dim_rk,branch_cd,sap_doc_type,reference_header,post_dt
		,doc_dt,doc_type,doc_no,accode,dc_flg,actype ,posting_amt
		,system_type,transaction_type,premium_type,policy_no,plan_cd,rider_cd
		,pay_dt,pay_by_channel,sum_natrn_amt,posting_sap_amt,posting_proxy_amt
		,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group
		,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
		,source_filename,group_nm,subgroup_nm,subgroup_desc
		,variable_cd,variable_nm,nadet_detail
		,org_branch_cd,org_submit_no,submit_no,section_order_no,is_section_order_no
		,for_branch_cd,event_type,premium_gmm_flg,premium_gmm_desc,policy_type
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
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,natrn.sales_id,natrn.sales_struct_n
			,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
			,natrn.natrn_dim_txt as nac_dim_txt
			,natrn.filename as source_filename  
			, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
			, case when natrn.nadet_policyno is not null then coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') else coa.dummy_variable_nm end as variable_cd
			, coalesce (vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
			, natrn.detail as nadet_detail 
			, natrn.org_branch_cd, null::varchar as org_submit_no
			, null::varchar as submit_no
			, null::varchar as section_order_no
			, 0 as is_section_order_no
			,natrn.for_branch_cd
			,natrn.event_type 
			,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
									order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd,vn.variable_name) as _rk 
			from  stag_a.stga_ifrs_nac_txn_missing_step05_premium natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )   
			left outer join dds.tl_acc_policy_chg pol  
		 	on ( natrn.nadet_policyno  = pol.policy_no 
		 	-- Oil 25jan2023 : เพิ่ม plan_cd 
		 	-- 13Feb2023 Narumol W. : comment nac.plancd & add pol.policy_type
		 	--and nac.plan_cd = pol.plan_cd 
		 	and pol.policy_type not in ('M','G','B')
		 	and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			left join  stag_s.stg_tb_core_planspec pp 
			on ( pol.plan_cd = pp.plan_cd)
			left outer join stag_s.ifrs_common_coa coa 
			on ( natrn.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Premium' 
			and natrn.premium_gmm_flg  = vn.premium_gmm_flg
			and vn.variable_cd   in ( 'V1','V7') --Log.179 
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
			where  natrn.post_dt  between v_control_start_dt and v_control_end_dt
			and natrn.event_type = 'Premium' --log.194
			and natrn.doc_no not like '5%' --log.196
			and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn 
							    where txn.branch_cd = natrn.branch_cd 
								and txn.doc_dt = natrn.doc_dt 
								and txn.doc_type = natrn.doc_type
								and txn.doc_no = natrn.doc_no
								and txn.accode = natrn.accode
								and txn.nac_dim_txt = natrn.natrn_dim_txt -- เพิ่ม natrn_dim_txt 
								and txn.variable_cd = 'V1') --log.201 30Aug2022 Oil : Add condition dummy
			
			union 
			select natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
			,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
			,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
			,natrn.natrn_amt as nac_amt
			,natrn.posting_natrn_amt as posting_amt
			,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
			,natrn.nadet_policyno as policy_no,coalesce (natrn.plan_cd,pol.plan_cd) as plan_cd
			,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,natrn.sales_id,natrn.sales_struct_n
			,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
			,natrn.natrn_dim_txt as nac_dim_txt
			,natrn.filename as source_filename  
			, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
			, case when natrn.nadet_policyno is not null then coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') else coa.dummy_variable_nm end as variable_cd
			, coalesce (vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
			, natrn.detail as nadet_detail 
			, natrn.org_branch_cd, null::varchar as org_submit_no
			, null::varchar as submit_no
			, null::varchar as section_order_no
			, 0 as is_section_order_no
			,natrn.for_branch_cd
			,natrn.event_type 
			,natrn.premium_gmm_flg ,natrn.premium_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg ,posting_natrn_amt
									order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd,vn.variable_name ) as _rk 
			from  stag_a.stga_ifrs_nac_txn_step04_prem   natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )  
			left outer join dds.tl_acc_policy_chg pol  
		 	on ( natrn.nadet_policyno  = pol.policy_no 
		 	and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			left join  stag_s.stg_tb_core_planspec pp 
			on ( pol.plan_cd = pp.plan_cd)
			left outer join stag_s.ifrs_common_coa coa 
			on ( natrn.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Premium' 
			and vn.variable_cd   in ( 'V1','V7') -- Log.179 
			and natrn.premium_gmm_flg  = vn.premium_gmm_flg )	 
			where  natrn.doc_no like '5%'
			and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_premium txn  --Oil 07-04-2023 : Log.355 
							    where txn.branch_cd = natrn.branch_cd 
								and txn.doc_dt = natrn.doc_dt 
								and txn.doc_type = natrn.doc_type
								and txn.doc_no = natrn.doc_no
								and txn.accode = natrn.accode
								and txn.nac_dim_txt = natrn.natrn_dim_txt -- เพิ่ม natrn_dim_txt 
								and txn.variable_cd in ('V1','V7')) 			 
			and natrn.event_type = 'Premium' --log.194
			and pol.policy_no is null 
			and natrn.post_dt  between v_control_start_dt and v_control_end_dt

		)  as aa 
		where _rk =1 ; 
	 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP35 VX Dummy: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP35 % : % row(s) - VX - Dummy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

	--=============== Exclude ====================--
 	begin   

	 	delete from stag_a.stga_ifrs_nac_txn_step05_premium_geb
	 	where post_dt  between  v_control_start_dt and v_control_end_dt;
	 	raise notice 'STEP36.1 % : % row(s) - delete stga_ifrs_nac_txn_step05_premium_geb',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 	
	 	--- Prep data for GWP 
	 	-- 16Feb2024 Narumol W.: [ITRQ#67010275] Enhance GL_ACCOUNT_BALANCE_SEGMENT_GWP : Reject RID,RRID
	 	-- 06Jul2024 Narumol W.: [ITRQ#67062844] Enhance Variable Premium Condition for Unit Linked Product 
	 	-- 16Oct2024 Nuttadet O.: [ITRQ#67104925] ปรับเพิ่มเงื่อนไข Variable Premium Condition for Unit Linked Product (system_type = 'SC')
	 	insert into stag_a.stga_ifrs_nac_txn_step05_premium_geb  
	 	select *
	 	from stag_a.stga_ifrs_nac_txn_step05_premium 
	 	where  transaction_type  in ( 'RID','RRID' ) 
	    and system_type = 'SC'; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP36.2 % : % row(s) - Insert gwp RID ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	
	 
	 	insert into stag_a.stga_ifrs_nac_txn_step05_premium_geb  
	 	select *
	 	from stag_a.stga_ifrs_nac_txn_step05_premium 
	 	where plan_cd   in (  'M907','PL34') ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP36.3 % : % row(s) - Insert gwp group eb ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

		-- 20Jan22 Narumol W.: GWP LogNo.9 N'Kate cf condition GEB to product_group=17 & product_sub_group=704
	 	-- 22Nov22 : Replace condition for GROUPEB is plan_cd = 798 
	 	insert into stag_a.stga_ifrs_nac_txn_step05_premium_geb  
	 	select *
	 	from stag_a.stga_ifrs_nac_txn_step05_premium 
	 	-- where branch_cd = '798'
	 	where product_group='17' 
		and product_sub_group='704';
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP36.4 % : % row(s) - Insert gwp group eb ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

	 /*
	 	insert into stag_a.stga_ifrs_nac_txn_step05_premium_geb  
	 	select *
	 	from stag_a.stga_ifrs_nac_txn_step05_premium 
	 	where event_type  = 'Premium_GWP';
	 */
	 ------------------
	 	-- Log.180 Exclude ไม่เอารายการคืนเบี้ยของแบบประกัน Unit Linked ที่ transaction_type = "RID" or "RRID" เข้า mapping variable  (log.180)
		-- 26May2023 Narumol W. : ITRQ-66052190: ขอปรับเงื่อนไข Mapping Variable (IFRS17) เพื่อนำรายการเบี้ยชีวิตเพิ่มเติม/ ยกเลิกเบี้ยชีวิตเพิ่มเติม (RID) เข้า PREMIUM_YTD
	 	-- 06Jul2024 Narumol W.: [ITRQ#67062844] Enhance Variable Premium Condition for Unit Linked Product 
	 	-- 16Oct2024 Nuttadet O.: [ITRQ#67104925] ปรับเพิ่มเงื่อนไข Variable Premium Condition for Unit Linked Product (system_type = 'SC') 
 		delete from stag_a.stga_ifrs_nac_txn_step05_premium 
		where transaction_type  in ( 'RID','RRID' ) 
	    and system_type = 'SC';
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP30.3 % : % row(s) - delete gwp RID ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
 	
	 	-- Log.66 Exclude PAA
		delete from stag_a.stga_ifrs_nac_txn_step05_premium 
		where plan_cd   in (  'M907','PL34' ) ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP37.1 % : % row(s) - delete gwp group eb ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
  
		delete from stag_a.stga_ifrs_nac_txn_step05_premium 
		where event_type  = 'Premium_GWP';
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP37.2 % : % row(s) - delete Premium_GWP ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 
		-- 07Sep2022 Narumol W.:Premium Log 203 Exclude is_cutoff = 1 
		delete from stag_a.stga_ifrs_nac_txn_step05_premium p
		where event_type  = 'Premium' 
		and exists ( select 1 from  dds.ifrs_suspense_newcase_chg nc
					where doc_dt  between v_control_start_dt and v_control_end_dt 
					and p.doc_dt = nc.doc_dt 
					and p.doc_type = nc.doc_type 
					and p.doc_no = nc.doc_no 
					and p.accode = nc.c_accode 
					and nc.is_cut_off = '1'  );
				
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP37.3 % : % row(s) - delete Premium_GWP ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
				
		-- 07Sep2022 Oil : update นำรายการที่เป็นของ M907 ออก Log.178		
		delete from stag_a.stga_ifrs_nac_txn_step05_premium p		
		where event_type  = 'Premium' 
		and sap_doc_type in ('SA','SX')
		and product_sub_group ='702'
		and cost_center ='0016130100'
		and accode in ('4011020040','4011020043')
		and post_dt  between v_control_start_dt and v_control_end_dt;
				
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP30.4 % : % row(s) - Update Exclude M907 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 	
	 	-- 15Dec2022 Oil :Log.254 Exclude product_group='17' and product_sub_group	='704'
		-- 20Jan22 Narumol W.: GWP LogNo.9 N'Kate cf condition GEB to product_group=17 & product_sub_group=704
		delete from stag_a.stga_ifrs_nac_txn_step05_premium p		
		where product_group='17' 
		and product_sub_group='704'
		and post_dt  between v_control_start_dt and v_control_end_dt;
	
		-- 02Oct2023 Narumol W. : Patch policy_no and plan_cd
		update stag_a.stga_ifrs_nac_txn_step05_premium p
		set  policy_no = pol.policy_no 
		,plan_cd = pol.plan_cd
		from dds.tl_acc_policy_chg pol 
		where ( p.policy_no = pol.plan_cd || pol.policy_no 
		and p.doc_dt between pol.valid_fr_dttm  and pol.valid_to_dttm ) 
		and length(p.policy_no) = 12 
		and p.sap_doc_type in ('SI','SB');
				
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	 	raise notice 'STEP30.4 % : % row(s) - Exclude product_group /product_sub_group',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	 	
	 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP30 Exclude GROUPEB: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	--raise notice 'STEP30 % : % row(s) - Exclude GROUPEB ',clock_timestamp()::varchar(19),v_affected_rows::varchar;		  

	-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
	begin
		
		if date_part('month',v_control_start_dt) = 1  then  
		
		    insert into stag_a.stga_ifrs_nac_txn_step05_premium  
			select ref_1 ,nac_rk ,natrn_x_dim_rk ,branch_cd
			 ,sap_doc_type ,reference_header 
			 ,v_control_end_dt as post_dt
			 ,doc_dt
			 ,doc_type ,doc_no ,accode ,dc_flg ,actype
			 ,posting_amt * -1 as posting_amt
			 ,system_type ,transaction_type ,premium_type
			 ,policy_no ,plan_cd ,rider_cd ,pay_dt ,pay_by_channel
			 ,sum_natrn_amt ,posting_sap_amt ,posting_proxy_amt
			 ,sales_id ,sales_struct_n ,selling_partner ,distribution_mode
			 ,product_group ,product_sub_group ,rider_group
			 ,product_term ,cost_center ,nac_dim_txt ,source_filename
			 ,group_nm ,'Accrued_bop' as subgroup_nm ,subgroup_desc
			 ,variable_cd ,variable_nm ,nadet_detail ,org_branch_cd
			 ,org_submit_no ,submit_no ,section_order_no ,is_section_order_no
			 ,for_branch_cd ,event_type ,premium_gmm_flg ,premium_gmm_desc
			 ,policy_type ,effective_dt ,issued_dt
			 ,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode
			 ,ifrs17_portid ,ifrs17_portgroup ,is_duplicate_variable 
			 ,log_dttm 
			 ,fee_cd
			 from dds.ifrs_variable_premium_ytd a
			 where a.control_dt = v_control_end_dt - interval '1 month'
			 and variable_nm in ('ACTUAL_PRM_CH_CL_CS','ACTUAL_PRM_CH_CL_FS')
			 and source_filename = 'accrued-ac';
 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		end if;
			
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.fn_ifrs_variable_txn05_premium' 				
				,'ERROR STEP31 VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP31 % : % row(s) - VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด',clock_timestamp()::varchar(19),v_affected_rows::varchar;			

	--=============== Insert into YTD ==================--
        select dds.fn_ifrs_variable_premium(p_xtr_start_dt) into out_err_cd  ; 

 	-- Completes
	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.fn_ifrs_variable_txn05_premium','COMPLETE: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

   	return i;
end

$function$
;
