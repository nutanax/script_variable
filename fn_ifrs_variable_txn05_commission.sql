CREATE OR REPLACE FUNCTION dds.fn_ifrs_variable_txn05_commission(p_xtr_start_dt date)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
-- =============================================  
-- Author:  Narumol W.
-- Create date: 2023-01-25
-- Description: For insert event data 
-- 15Dec2022 : กรอง doctype not in ( 'SI','SB') & not exists in V3,ยกเลิก Filter non-agent,Doctype = 'SX' ไม่เอาที่กลับรายงาน SA ของ V3 เดือนที่แล้ว
-- 14Dec2022 : K.Ked ยกเลิก V4 ไปรวมกับ V11 ไม่แยก Non-agent/Agent
-- 24Jan2023 : V2 K.Ball ปรับ Condtion เป็น doc_type not in ( SI , SB ) 
-- 24Jan2023 : K.Ball ยกเลิก V3
-- 24Jan2023 : K.Ball ยกเลิก V6
-- Oil 25jan2023 : policy_type not in ( 'M','G')
-- Oil 25jan2023 : add plan_cd 
-- Oil 10Feb2023 : Add V5, V10 ovdebt , ovdebt void
 --29Feb2023 :Edit Condition for get all commission transaction 
-- Oil 17Feb2023 : Change source stag_f.ifrs_snewcase_chg to dds.ifrs_imp_snewcase
-- 21Feb2023 Narumol W. Log 297 Filter txn_dt <= end_dt  
-- 21Feb2023 Narumol W. : filter control_dt for get all transaction untill current
-- 06Mar2023 Narumol W.: Log 347 least(pol.valid_fr_dttm,pol.effective_dt)
-- 20Mar2023 Narumol W : Commission Log 352Commission Log 352 Add Key join sales_id
-- 22Mar2023 Narumol W.: Chg.87 add source payment_trans for CT_AUTO
-- 22Mar2023 Narumol W.: Chg.88 add source nac for จ่ายค่าจัดงานประจำเดือน
-- Oil 20230516 : Add V13 stag_f.tb_sap_trial_balance >> ปรับใช้  dds.tb_sap_trial_balance
-- 23May2023 Narumol W.: ITRQ-66052187 ขอเพิ่ม source suspense newcase ใน V11 V12 ของ COM_YTD
-- 24May2023 Narumol W.: ITRQ-66052189 ขอเพิ่มเงื่อนไข Var.14 COM_YTD (IFRS17)
-- 19Jul2023 Narumol W.: Log. 369 III = II ทั้งหมด ของ DEC ปีที่แล้ว  
-- 20Jul2023 Narumol W.: Commission Log 364 Exclude is_cutoff = 1 
-- 31Jul2023 Narumol W.: Commission Log.362 : Use ntrn.branch_cd instead of sus.branch_cd  
-- 14Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
-- 02Oct2023 Narumol W.: Patch policy_no and plan_cd
-- 06Feb2024 Narumol W.: [ITRQ#67010482]CR_Enhance V14 COMMISSION_YTD Data
-- 29Oct2024 Nuttadet O. : [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
-- 14Jan2025 Nuttadet O. : log.Com464 add key join dc_flg
-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
-- =============================================  
declare 
	i int default  0;
	v_control_start_dt date;
	v_control_end_dt date;
	v_control_boy_dt date;
	out_err_cd int default 0;
	out_err_msg varchar(200);
	--out_cd void;

	v_affected_rows int default 0 ;
	v_diff_sec int;
	v_current_time timestamp;

	v_start_eoy_dt date;
	v_end_eoy_dt date;

BEGIN 
	
	
--  select dds.fn_ifrs_variable_txn05_commission('2022-03-20'::date) 
--  select dds.fn_ifrs_variable_txn05_commission('2024-02-20'::date) 
--  select dds.fn_ifrs_variable_txn05_commission('2022-01-20'::date);
--  select  * from  dds.tb_fn_log order by log_dttm desc
	
	raise notice 'COMMISSION - START % : % row(s) - ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.ifrs_variable_txn05_commission','START: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

	if p_xtr_start_dt is not null then 
		v_control_start_dt := date_trunc('month', p_xtr_start_dt - interval '1 month');
		v_control_end_dt := v_control_start_dt + interval '1 month -1 day' ;
		v_control_boy_dt := date_trunc('year', p_xtr_start_dt);
		v_start_eoy_dt := date_trunc('year', v_control_start_dt) - interval '1 month' ;
		v_end_eoy_dt := date_trunc('year', v_control_start_dt)- interval '1 day' ;
	END if;
 
	raise notice 'v_control_start_dt :: % ', v_control_start_dt::VARCHAR(10);
	raise notice 'v_control_end_dt :: % ', v_control_end_dt::VARCHAR(10); 
	raise notice 'v_control_boy_dt :: % ', v_control_boy_dt::VARCHAR(10); 
	raise notice 'v_start_eoy_dt :: % ', v_start_eoy_dt::VARCHAR(10); 
	raise notice 'v_end_eoy_dt :: % ', v_end_eoy_dt::VARCHAR(10); 

	-- Prepare accrual 
 	select dds.fn_dynamic_vw_ifrs_accrual_chg (v_control_start_dt , v_control_end_dt) into out_err_cd;
	select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg(v_control_start_dt) into out_err_cd;

  	truncate table  stag_a.stga_ifrs_nac_txn_step05_commission;
   
	-- 15Dec2022 : เพิ่มเงื่อนไข reverse_with is missing ,ยกเลิก Filter non-agent,ลบเดือนก่อนหน้าออก
  	-- 24Jan2023 : K.Ball ยกเลิก V3
	/*
 	----------------------NON AGENT -  V3
  	BEGIN
		insert into stag_a.stga_ifrs_nac_txn_step05_commission
			(ref_1,nac_rk,branch_cd,sap_doc_type
			,reference_header,post_dt,doc_dt,doc_no
			,accode,dc_flg,posting_amt
			,policy_no,plan_cd
			,sum_natrn_amt,posting_sap_amt,posting_proxy_amt 
			,selling_partner,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center
			,nac_dim_txt,source_filename,group_nm,subgroup_nm,subgroup_desc
			,variable_cd,variable_nm,nadet_detail
			,event_type,comm_gmm_flg,comm_gmm_desc,policy_type,effective_dt,issued_dt
			,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable)
		select sap.ref1_header
			,sap.line_item as nac_rk 
			,right(sap.processing_branch_cd,3) as branch_cd 
			,sap.doc_type as sap_doc_type 
			,sap.reference_header
			,(date_trunc('month',sap.posting_date_dt) + interval '1 month -1 day')::date as posting_date_dt
			,(date_trunc('month',sap.doc_dt) + interval '1 month -1 day')::date as  doc_dt,sap.doc_no
			,sap.account_no as accode ,sap.dc_flg
			,sap.posting_sap_amt as posting_amt 
			,sap.policy_no , coalesce(sap.plan_cd,pol.plan_cd) as plan_cd
			,sap.posting_sap_amt as sum_natrn_amt 
			,sap.posting_sap_amt as posting_sap_amt 
			,sap.posting_sap_amt as posting_proxy_amt  
			,sap.selling_partner,sap.distribution_mode,sap.product_group,sap.product_sub_group,sap.rider_group,sap.term_cd,right(sap.cost_center,8) cost_center
			,sap_dim_txt as nac_dim_txt 
			,'sap' as source_filename , 'sap' as  group_nm  ,  'Accruedac_current' as subgroup_nm 
			,'V3 Accrued Comm'  as subgroup_desc
			, vn.variable_cd, vn.variable_name as variable_nm  
			, sap.description_txt  
			, coa.event_type , coa.comm_gmm_flg , coa.comm_gmm_desc 
			, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			from stag_a.stga_ifrs_nac_txn_step01  sap
			inner join stag_s.ifrs_common_coa coa 
			on ( sap.account_no = coa.accode 
			and coa.event_type  = 'Commission')
			left outer join dds.tl_acc_policy_chg pol  
			on ( sap.policy_no  = pol.policy_no
			and sap.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			left join  stag_s.stg_tb_core_planspec pp 
			on ( pol.plan_cd = pp.plan_cd) 
			inner join stag_s.ifrs_common_variable_nm vn
			on ( vn.event_type = 'Commission'
			and vn.variable_cd = 'V3'
			and coa.comm_gmm_flg = vn.comm_gmm_flg
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )     
			where sap.posting_date_dt between v_control_start_dt and v_control_end_dt
			and sap.doc_type ='SA'
			and sap.assignment_code = 'A'
			and date_part('day',sap.posting_date_dt) >= 28
			--15Dec2022 Narumol W.: เพิ่มเงื่อนไข reverse_with is missing ,ยกเลิก Filter non-agent
			and coalesce(sap.reverse_doc_no,'') = ''; 
             
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP03 NON AGENT - V3-Accrued Comm monthly: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP03 % : % row(s) - NON AGENT - V3-Accrued Comm monthly',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 */ 
 
  	-- 14Dec2022 : K.Ked ยกเลิก V4 ไปรวมกับ V11 ไม่แยก Non-agent/Agent 
 	---------------------NON AGENT - - V2
	-- 15Dec2022 : กรอง doctype not in ( 'SI','SB') & not exists in V3 ,ยกเลิก Filter non-agent,Doctype = 'SX' ไม่เอาที่กลับรายงาน SA ของ V3 เดือนที่แล้ว
	-- 24Jan2023 : V2 K.Ball ปรับ Condtion เป็น doc_type not in ( SI , SB ) 

  	BEGIN 
		insert into stag_a.stga_ifrs_nac_txn_step05_commission
		(ref_1,nac_rk,branch_cd,sap_doc_type
		,reference_header,post_dt,doc_dt,doc_no
		,accode,dc_flg,posting_amt
		,policy_no,plan_cd
		,sum_natrn_amt,posting_sap_amt,posting_proxy_amt 
		,selling_partner,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center
		,nac_dim_txt,source_filename,group_nm,subgroup_nm,subgroup_desc
		,variable_cd,variable_nm,nadet_detail
		,event_type,comm_gmm_flg,comm_gmm_desc,policy_type,effective_dt,issued_dt
		,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable
		,rider_cd)
		select sap.ref1_header
		,sap.line_item as nac_rk 
		,right(sap.processing_branch_cd,3) as branch_cd 
		,sap.doc_type as sap_doc_type 
		,sap.reference_header
		,(date_trunc('month',sap.posting_date_dt) + interval '1 month -1 day')::date as posting_date_dt
		,(date_trunc('month',sap.doc_dt) + interval '1 month -1 day')::date as  doc_dt,sap.doc_no
		,sap.account_no as accode ,sap.dc_flg
		,sap.posting_sap_amt as posting_amt 
		,sap.policy_no , coalesce(sap.plan_cd,pol.plan_cd) as plan_cd
		,sap.posting_sap_amt as sum_natrn_amt 
		,sap.posting_sap_amt as posting_sap_amt 
		,sap.posting_sap_amt as posting_proxy_amt  
		,sap.selling_partner,sap.distribution_mode,sap.product_group,sap.product_sub_group,sap.rider_group,sap.term_cd,right(sap.cost_center,8) cost_center
		,sap_dim_txt as nac_dim_txt 
		,'sap' as source_filename , 'sap' as  group_nm  , 'V2 Accrued Comm' as subgroup_nm 
		,'รับรู้ Commission จาก Policy ที่ได้รับการอนุมัติแล้ว/ จ่ายค่า commission เป็นเงินสดตอนที่มีกรมธรรมแล้ว (Non Agent)'  as subgroup_desc
		, coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') as variable_cd -- Log.303
		, coalesce (vn.variable_name ,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm -- Log.303
		, sap.description_txt  
		, coa.event_type , coa.comm_gmm_flg , coa.comm_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		,sap.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd) 
		from stag_a.stga_ifrs_nac_txn_step01 sap 
		inner join stag_s.ifrs_common_coa coa 
		on ( sap.account_no = coa.accode 
		and coa.event_type  = 'Commission')
		left outer join dds.tl_acc_policy_chg pol  
		on ( sap.policy_no  = pol.policy_no
		and sap.plan_cd  = pol.plan_cd -- Oil 25jan2023 : add plan_cd
		--06Mar2023 Narumol W.: Log 347 least(pol.valid_fr_dttm,pol.effective_dt)
		and sap.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd) 
		left join stag_s.ifrs_common_variable_nm vn -- inner to left -- Logด.303
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V2'
		and coa.comm_gmm_flg = vn.comm_gmm_flg
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )     
		where sap.posting_date_dt between  v_control_start_dt and v_control_end_dt  
		-- 15Dec2022 : กรอง doctype not in ( 'SI','SB') & not exists in V3 ,ยกเลิก Filter non-agent,Doctype = 'SX' ไม่เอาที่กลับรายงาน SA ของ V3 เดือนที่แล้ว
		-- 24Jan2023 : V2 K.Ball ปรับ Condtion เป็น doc_type not in ( SI , SB ) 
		and sap.doc_type not in ( 'SI','SB');
		-- 24Jan2023 : เนื่องจากยกเลิก V3
		/*and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_commission c 
							where sap.ref1_header = c.ref_1 
							and sap.doc_dt = c.doc_dt
							and sap.doc_type = c.sap_doc_type 
							and sap.account_no = c.accode); 
		*/
 		--and not exists ( select 1 from stag_s.ifrs_common_partner_dim pt where selling_partner_desc = 'Agency' and pt.selling_partner = sap.selling_partner) ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;			
		raise notice 'STEP05.1 % : % row(s) - V2-Accrued Comm monthly ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	 
		-- 15Dec2022 : Doctype = 'SX' ไม่เอาที่กลับรายงาน SA ของ V3 เดือนที่แล้ว
		-- 24Jan2023 : V2 K.Ball ปรับ Condtion เป็น doc_type not in ( SI , SB ) -> ยกเลิก Doctype = 'SX' ไม่เอาที่กลับรายงาน SA ของ V3 เดือนที่แล้ว
		/*
		insert into stag_a.stga_ifrs_nac_txn_step05_commission
		(ref_1,nac_rk,branch_cd,sap_doc_type
		,reference_header,post_dt,doc_dt,doc_no
		,accode,dc_flg,posting_amt
		,policy_no,plan_cd
		,sum_natrn_amt,posting_sap_amt,posting_proxy_amt 
		,selling_partner,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center
		,nac_dim_txt,source_filename,group_nm,subgroup_nm,subgroup_desc
		,variable_cd,variable_nm,nadet_detail
		,event_type,comm_gmm_flg,comm_gmm_desc,policy_type,effective_dt,issued_dt
		,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable)
		select sap.ref1_header
		,sap.line_item as nac_rk 
		,right(sap.processing_branch_cd,3) as branch_cd 
		,sap.doc_type as sap_doc_type 
		,sap.reference_header
		,(date_trunc('month',sap.posting_date_dt) + interval '1 month -1 day')::date as posting_date_dt
		,(date_trunc('month',sap.doc_dt) + interval '1 month -1 day')::date as  doc_dt,sap.doc_no
		,sap.account_no as accode ,sap.dc_flg
		,sap.posting_sap_amt as posting_amt 
		,sap.policy_no , coalesce(sap.plan_cd,pol.plan_cd) as plan_cd
		,sap.posting_sap_amt as sum_natrn_amt 
		,sap.posting_sap_amt as posting_sap_amt 
		,sap.posting_sap_amt as posting_proxy_amt  
		,sap.selling_partner,sap.distribution_mode,sap.product_group,sap.product_sub_group,sap.rider_group,sap.term_cd,right(sap.cost_center,8) cost_center
		,sap_dim_txt as nac_dim_txt 
		,'sap' as source_filename , 'sap' as  group_nm  , 'V2 Accrued Comm' as subgroup_nm 
		,'รับรู้ Commission จาก Policy ที่ได้รับการอนุมัติแล้ว/ จ่ายค่า commission เป็นเงินสดตอนที่มีกรมธรรมแล้ว'  as subgroup_desc
		, vn.variable_cd, vn.variable_name as variable_nm  
		, sap.description_txt  
		, coa.event_type , coa.comm_gmm_flg , coa.comm_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from stag_a.stga_ifrs_nac_txn_step01  sap
		inner join stag_s.ifrs_common_coa coa 
		on ( sap.account_no = coa.accode 
		and coa.event_type  = 'Commission')
		left outer join dds.tl_acc_policy_chg pol  
		on ( sap.policy_no  = pol.policy_no
		and sap.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V2'
		and coa.comm_gmm_flg = vn.comm_gmm_flg
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )     
		where sap.posting_date_dt between  v_control_start_dt and v_control_end_dt   
		and sap.doc_type = 'SX' 
		and reverse_doc_no in ( select doc_no from dds.ifrs_variable_commission_ytd  
								where doc_type = 'SA' and variable_cd ='V3'
								and posting_date_dt between v_control_start_dt - interval '1 month' and v_control_start_dt - interval '1 day' )  
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_commission c 
							where sap.ref1_header = c.ref_1 
							and sap.doc_dt = c.doc_dt
							and sap.doc_type = c.sap_doc_type 
							and sap.account_no = c.accode); 		 
	  	 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;			
		raise notice 'STEP05.2 % : % row(s) - V2-Accrued Comm monthly - SX not reverse of SA in previous month ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		*/
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP05 - V2-Accrued Comm monthly Non-Agent: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP05 % : % row(s) - - V2-Accrued Comm monthly ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  
 	--========================================AGENT=====================================================================================--
	---- I : Comm Expense : เอา com exp ทั้งหมด เข้า com 17 
	BEGIN     
		insert into  stag_a.stga_ifrs_nac_txn_step05_commission 
		select  natrn.ref_1 as ref_1 , comm.nac_rk , natrn.natrn_x_dim_rk  
		,comm.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,comm.doc_dt,comm.doc_type,comm.doc_no,comm.accode,natrn.dc_flg,null::varchar actype  
		,comm.posting_comm_amt 
		,comm.system_type,comm.transaction_type, null::varchar premium_type 
		,comm.temp_policy_no , comm.plan_cd, comm.rider_cd  ,comm.pay_dt ,null::varchar pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,comm.nac_dim_txt 
		,comm.source_file  as source_filename , 'natrn-agent_comm' as  group_nm  , 'I-Comm expense' as subgroup_nm , 'I-Comm expense'  as subgroup_desc 
		, 'I' as  variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, comm.submit_no as org_submit_no
		, comm.rpno as receipt_no, comm.submit_no
		, null::varchar section_order_no ,0 is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		--,pp.ifrs17_var_current,pp.ifrs17_var_future
		from  stag_a.stga_ifrs_nac_txn_step04 as natrn 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_agent_comm_chg  comm    
		on ( natrn.branch_cd = comm.branch_cd 
        and natrn.doc_dt = comm.doc_dt 
        and natrn.doc_type = comm.doc_type
        and natrn.doc_no = comm.doc_no 
        and natrn.accode = comm.accode  
        and natrn.natrn_dim_txt = comm.nac_dim_txt 
        and natrn.dc_flg = comm.dc_flg ) -- 14Jan2025 Nuttadet O.: log.Com464 add key join dc_flg
		left join  stag_s.stg_tb_core_planspec pp 
		on ( comm.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( comm.temp_policy_no = pol.policy_no 
		and comm.plan_cd  = pol.plan_cd
		and comm.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V10'  
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg  = vn.comm_gmm_flg 	
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )			 
		where natrn.post_dt between  v_control_start_dt and v_control_end_dt;   
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP07 I : Comm Expense  : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP07 % : % row(s) - AGENT-I : Comm Expense',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  
	---- II : Prepaid comm current

    -- 29Feb2023 :Edit Condition for get all commission transaction     
	BEGIN   
        -- Prepaid commission
		select dds.fn_ifrs_prepaid_commission(v_control_end_dt)  into out_err_cd ;
    
		insert into  stag_a.stga_ifrs_nac_txn_step05_commission 
		select p.ref_1,p.nac_rk,p.natrn_x_dim_rk,p.branch_cd,p.sap_doc_type
		,p.reference_header,p.post_dt,p.doc_dt,p.doc_type,p.doc_no,p.accode,p.dc_flg
		,p.actype,p.posting_amt,p.system_type,p.transaction_type,p.premium_type
		,p.policy_no,p.plan_cd,p.rider_cd,p.pay_dt,p.pay_by_channel
		,p.sum_natrn_amt,p.posting_sap_amt,p.posting_proxy_amt
		,p.sales_id,p.sales_struct_n,p.selling_partner,p.distribution_mode
		,p.product_group,p.product_sub_group,p.rider_group,p.product_term
		,p.cost_center,p.nac_dim_txt,p.source_filename
		,p.group_nm,p.subgroup_nm,p.subgroup_desc,p.variable_cd,p.variable_nm
		,p.nadet_detail,p.org_branch_cd,p.org_submit_no,p.rp_no,p.submit_no
		,p.section_order_no,p.is_section_order_no,p.for_branch_cd
		,p.event_type,p.comm_gmm_flg,p.comm_gmm_desc,p.policy_type
		,p.effective_dt,p.issued_dt,p.ifrs17_channelcode,p.ifrs17_partnercode
		,p.ifrs17_portfoliocode,p.ifrs17_portid,p.ifrs17_portgroup
		,p.is_duplicate_variable --,p.log_dttm,p.control_dt
		from dds.ifrs_prepaid_commission p
		-- 21Feb2023 Narumol W. : filter control_dt for get all transaction untill current
		where p.control_dt <= v_control_end_dt
		and exists ( select 1 from  dds.ifrs_imp_snewcase sn 
						where p.policy_no  = sn.temp_policy_no
						and sn.report_dt = v_control_end_dt ); 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
			,'ERROR STEP08 AGENT-V10-II : Prepaid comm current : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP08 % : % row(s) - AGENT-II : Prepaid comm current',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
       /*
		insert into  stag_a.stga_ifrs_nac_txn_step05_commission  
		select  natrn.ref_1 as ref_1 , comm.nac_rk , natrn.natrn_x_dim_rk  
		,comm.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,comm.doc_dt,comm.doc_type,comm.doc_no,comm.accode,natrn.dc_flg,null::varchar actype  
		,comm.posting_comm_amt 
		,comm.system_type,comm.transaction_type, null::varchar premium_type 
		,comm.policy_no , comm.plan_cd, comm.rider_cd  ,comm.pay_dt ,null::varchar pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,comm.nac_dim_txt 
		,comm.source_file  as source_filename , 'natrn-agent_comm' as  group_nm  , 'II-Prepaid commission' as subgroup_nm , 'II-Prepaid commission' as subgroup_desc   
		, 'II' as  variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, comm.submit_no as org_submit_no
		, comm.rpno as receipt_no, comm.submit_no
		, null::varchar section_order_no ,0 is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		--,pp.ifrs17_var_current,pp.ifrs17_var_future
		from  stag_a.stga_ifrs_nac_txn_step04 as natrn 
		left join stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_agent_comm_chg  comm    
		on ( natrn.branch_cd = comm.branch_cd 
        and natrn.doc_dt = comm.doc_dt 
        and natrn.doc_type = comm.doc_type
        and natrn.doc_no = comm.doc_no 
        and natrn.accode = comm.accode  
        and natrn.natrn_dim_txt = comm.nac_dim_txt  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( comm.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( comm.policy_no = pol.policy_no 
		and comm.plan_cd  = pol.plan_cd
		and comm.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V10'  
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg  = vn.comm_gmm_flg 	
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
		where exists ( select 1 from  stag_f.ifrs_snewcase_chg sn 
						where comm.temp_policy_no  = sn.x_policy_no
						and sn.doc_dt  between  v_control_start_dt and v_control_end_dt
						)
		and natrn.post_dt between v_control_start_dt and v_control_end_dt  ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	*/

	---- III : Prepaid comm eoy 
	
	BEGIN   
		IF date_part('month',v_control_start_dt) = 1 then 
	 
			 IF v_start_eoy_dt =  '2021-12-01'::Date then 
			 	insert into  stag_a.stga_ifrs_nac_txn_step05_commission  
				select  natrn.ref_1 as ref_1 , comm.nac_rk , natrn.natrn_x_dim_rk  
				,comm.branch_cd
				,natrn.sap_doc_type,natrn.reference_header,v_control_end_dt as post_dt --natrn.post_dt
				,comm.doc_dt,comm.doc_type,comm.doc_no,comm.accode,natrn.dc_flg,null::varchar actype  
				,comm.posting_comm_amt 
				,comm.system_type,comm.transaction_type, null::varchar premium_type 
				,comm.temp_policy_no , comm.plan_cd, comm.rider_cd  ,comm.pay_dt ,null::varchar pay_by_channel
				,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
				,natrn.sales_id,natrn.sales_struct_n,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
				,comm.nac_dim_txt 
				,comm.filename  as source_filename , 'natrn-agent_comm' as  group_nm  
				, 'III-Prepaid commission eoy' as subgroup_nm , 'III-Prepaid commission eoy' as subgroup_desc   
				, 'III' as  variable_cd
				, vn.variable_name as variable_nm 
				, natrn.detail as nadet_detail 
				, natrn.org_branch_cd, comm.submit_no as org_submit_no
				, comm.receipt_no as receipt_no, comm.submit_no 
				, null::varchar section_order_no ,0 is_section_order_no
				,natrn.for_branch_cd
				,natrn.event_type 
				,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
				,pol.policy_type ,pol.effective_dt ,pol.issued_dt
				,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
				,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
				--,pp.ifrs17_var_current,pp.ifrs17_var_future
				from  stag_a.stga_ifrs_nac_txn_step04 as natrn 
				left join stag_s.ifrs_common_accode acc 
				on ( natrn.accode = acc.account_cd  )
				inner join  stag_a.stga_ifrs_snewcase_202112  comm    
				on ( natrn.branch_cd = comm.branch_cd 
		        and natrn.doc_dt = comm.doc_dt 
		        and natrn.doc_type = comm.doc_type
		        and natrn.doc_no = comm.doc_no 
		        and natrn.accode = comm.accode  
		        and natrn.natrn_dim_txt = comm.nac_dim_txt  )  
				left join  stag_s.stg_tb_core_planspec pp 
				on ( comm.plan_cd = pp.plan_cd) 
				left outer join dds.tl_acc_policy_chg pol  
				on ( comm.temp_policy_no = pol.policy_no 
				and comm.plan_cd  = pol.plan_cd
				and comm.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
				inner join stag_s.ifrs_common_variable_nm vn 
				on ( vn.event_type = 'Commission'
				and vn.variable_cd = 'V10'  
				and natrn.event_type  = vn.event_type
				and natrn.comm_gmm_flg  = vn.comm_gmm_flg 	
				and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
				where exists ( select 1 from  dds.ifrs_imp_snewcase sn 
								where comm.temp_policy_no  = sn.temp_policy_no
								--21Feb2023 Narumol W. Log 297 Filter txn_dt <= end_dt 
								--and sn.txn_dt  between   v_start_eoy_dt and v_end_eoy_dt  )
								and sn.txn_dt <= v_end_eoy_dt)
				and natrn.post_dt between  v_start_eoy_dt and v_end_eoy_dt   ; 
				GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
				raise notice 'STEP08 % : % row(s) - AGENT-III-Prepaid commission eoy 2021 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
			
			else
				-- 19Jul2023 Narumol W.: Log. 369 III = II ทั้งหมด ของ DEC ปีที่แล้ว  
				insert into stag_a.stga_ifrs_nac_txn_step05_commission 
				 (  ref_1,nac_rk,natrn_x_dim_rk,branch_cd,sap_doc_type
				,reference_header,post_dt,doc_dt,doc_type,doc_no,accode,dc_flg,actype
				,posting_amt,system_type,transaction_type,premium_type,policy_no,plan_cd,rider_cd
				,pay_dt,pay_by_channel,sum_natrn_amt,posting_sap_amt,posting_proxy_amt
				,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group
				,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
				,source_filename,group_nm,subgroup_nm,subgroup_desc,variable_cd,variable_nm
				,nadet_detail,org_branch_cd,org_submit_no,rp_no,submit_no,section_order_no,is_section_order_no
				,for_branch_cd,event_type,comm_gmm_flg,comm_gmm_desc,policy_type
				,effective_dt,issued_dt,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode
				,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable )	 
				 select  comm.ref_1,comm.nac_rk,comm.natrn_x_dim_rk,comm.branch_cd,comm.sap_doc_type
				,comm.reference_header,comm.post_dt,comm.doc_dt,comm.doc_type,comm.doc_no,comm.accode,comm.dc_flg,comm.actype
				,comm.posting_amt
				,comm.system_type,comm.transaction_type,comm.premium_type
				,comm.policy_no,comm.plan_cd,comm.rider_cd
				,comm.pay_dt,comm.pay_by_channel,comm.sum_natrn_amt,comm.posting_sap_amt,comm.posting_proxy_amt
				,comm.sales_id,comm.sales_struct_n,comm.selling_partner,comm.distribution_mode,comm.product_group
				,comm.product_sub_group,comm.rider_group,comm.product_term,comm.cost_center,comm.nac_dim_txt
				,comm.source_filename,comm.group_nm
				, 'III-Prepaid commission eoy' as subgroup_nm 
				, 'III-Prepaid commission eoy' as subgroup_desc   
				, 'III' as  variable_cd,comm.variable_nm
				,comm.nadet_detail,comm.org_branch_cd,comm.org_submit_no,comm.rp_no,comm.submit_no,comm.section_order_no,comm.is_section_order_no
				,comm.for_branch_cd,comm.event_type,comm.comm_gmm_flg,comm.comm_gmm_desc,comm.policy_type
				,comm.effective_dt,comm.issued_dt,comm.ifrs17_channelcode,comm.ifrs17_partnercode,comm.ifrs17_portfoliocode
				,comm.ifrs17_portid,comm.ifrs17_portgroup,comm.is_duplicate_variable  
				 from dds.ifrs_variable_commission_ytd comm  
				 where control_dt =  v_end_eoy_dt
				 and comm.subgroup_desc = 'II-Prepaid commission' 
				 and variable_cd = 'V5';
				
				GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
				raise notice 'STEP08 % : % row(s) - AGENT-III-Prepaid commission eoy',clock_timestamp()::varchar(19),v_affected_rows::varchar;
				 --CREATE INDEX ifrs_variable_commission_ytd_control_dt_idx3 ON dds.ifrs_variable_commission_ytd USING btree (control_dt, subgroup_nm,variable_cd);
 				/*
				insert into stag_a.stga_ifrs_nac_txn_step05_commission 
				select  natrn.ref_1 as ref_1 , comm.nac_rk , natrn.natrn_x_dim_rk  
				,comm.branch_cd
				,natrn.sap_doc_type,natrn.reference_header,v_control_end_dt as post_dt -- natrn.post_dt
				,comm.doc_dt,comm.doc_type,comm.doc_no,comm.accode,natrn.dc_flg,null::varchar actype  
				,comm.posting_comm_amt 
				,comm.system_type,comm.transaction_type, null::varchar premium_type 
				,comm.temp_policy_no , comm.plan_cd, comm.rider_cd  ,comm.pay_dt ,null::varchar pay_by_channel
				,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
				,natrn.sales_id,natrn.sales_struct_n,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
				,comm.nac_dim_txt 
				,comm.source_file  as source_filename , 'natrn-agent_comm' as  group_nm  
				, 'III-Prepaid commission eoy' as subgroup_nm , 'III-Prepaid commission eoy' as subgroup_desc   
				, 'III' as  variable_cd
				, vn.variable_name as variable_nm 
				, natrn.detail as nadet_detail 
				, natrn.org_branch_cd, comm.submit_no as org_submit_no
				, comm.rpno as receipt_no , comm.submit_no
				, null::varchar section_order_no ,0 is_section_order_no
				,natrn.for_branch_cd
				,natrn.event_type 
				,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
				,pol.policy_type ,pol.effective_dt ,pol.issued_dt
				,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
				,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable   
				--,pp.ifrs17_var_current,pp.ifrs17_var_future
				from  stag_a.stga_ifrs_nac_txn_step04 as natrn 
				left join stag_s.ifrs_common_accode acc 
				on ( natrn.accode = acc.account_cd  )
				inner join dds.ifrs_agent_comm_chg  comm    
				on ( natrn.branch_cd = comm.branch_cd 
		        and natrn.doc_dt = comm.doc_dt 
		        and natrn.doc_type = comm.doc_type
		        and natrn.doc_no = comm.doc_no 
		        and natrn.accode = comm.accode  
		        and natrn.natrn_dim_txt = comm.nac_dim_txt  )  
				left join  stag_s.stg_tb_core_planspec pp 
				on ( comm.plan_cd = pp.plan_cd) 
				left outer join dds.tl_acc_policy_chg pol  
				on ( comm.temp_policy_no = pol.policy_no 
				and comm.plan_cd  = pol.plan_cd
				and comm.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
				inner join stag_s.ifrs_common_variable_nm vn 
				on ( vn.event_type = 'Commission'
				and vn.variable_cd = 'V10'  
				and natrn.event_type  = vn.event_type
				and natrn.comm_gmm_flg  = vn.comm_gmm_flg 	
				and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
				where exists ( select 1 from  dds.ifrs_imp_snewcase sn 
								where comm.temp_policy_no  = sn.temp_policy_no
								--21Feb2023 Narumol W. Log 297 Filter txn_dt <= end_dt 
								--and sn.txn_dt  <= v_control_end_dt ) 
								and sn.txn_dt  between v_start_eoy_dt and v_end_eoy_dt)
				and natrn.post_dt between  v_start_eoy_dt and v_end_eoy_dt  ; 
				GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP08 % : % row(s) - AGENT-III-Prepaid commission eoy',clock_timestamp()::varchar(19),v_affected_rows::varchar;
			*/
			end if; 
	 	end if;
	 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP08 AGENT-V10-III-Prepaid commission eoy: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;   


	-- IV-Prepaid commission of previous month   
	-- ส่งทั้งฝั่ง +-
	 begin 
		 insert into stag_a.stga_ifrs_nac_txn_step05_commission 
		 (  ref_1,nac_rk,natrn_x_dim_rk,branch_cd,sap_doc_type
		,reference_header,post_dt,doc_dt,doc_type,doc_no,accode,dc_flg,actype
		,posting_amt,system_type,transaction_type,premium_type,policy_no,plan_cd,rider_cd
		,pay_dt,pay_by_channel,sum_natrn_amt,posting_sap_amt,posting_proxy_amt
		,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group
		,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
		,source_filename,group_nm,subgroup_nm,subgroup_desc,variable_cd,variable_nm
		,nadet_detail,org_branch_cd,org_submit_no,rp_no,submit_no,section_order_no,is_section_order_no
		,for_branch_cd,event_type,comm_gmm_flg,comm_gmm_desc,policy_type
		,effective_dt,issued_dt,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode
		,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable )	
		 select  comm.ref_1,comm.nac_rk,comm.natrn_x_dim_rk,comm.branch_cd,comm.sap_doc_type
		,comm.reference_header,comm.post_dt,comm.doc_dt,comm.doc_type,comm.doc_no,comm.accode,comm.dc_flg,comm.actype
		,comm.posting_amt,comm.system_type,comm.transaction_type,comm.premium_type
		,comm.policy_no,comm.plan_cd,comm.rider_cd
		,comm.pay_dt,comm.pay_by_channel,comm.sum_natrn_amt,comm.posting_sap_amt,comm.posting_proxy_amt
		,comm.sales_id,comm.sales_struct_n,comm.selling_partner,comm.distribution_mode,comm.product_group
		,comm.product_sub_group,comm.rider_group,comm.product_term,comm.cost_center,comm.nac_dim_txt
		,comm.source_filename,comm.group_nm
		,'IV-Prepaid commission of previous month'::varchar as subgroup_nm
		,'IV-Prepaid commission of previous month'::varchar as subgroup_desc
		,'IV'::varchar as variable_cd,comm.variable_nm
		,comm.nadet_detail,comm.org_branch_cd,comm.org_submit_no,comm.rp_no,comm.submit_no,comm.section_order_no,comm.is_section_order_no
		,comm.for_branch_cd,comm.event_type,comm.comm_gmm_flg,comm.comm_gmm_desc,comm.policy_type
		,comm.effective_dt,comm.issued_dt,comm.ifrs17_channelcode,comm.ifrs17_partnercode,comm.ifrs17_portfoliocode
		,comm.ifrs17_portid,comm.ifrs17_portgroup,comm.is_duplicate_variable  
		 from dds.ifrs_variable_commission_ytd comm 
		 inner join dds.tl_acc_policy_chg pol  
		 on ( comm.policy_no = pol.policy_no 
		 and comm.plan_cd  = pol.plan_cd
		 and pol.effective_dt is not null and pol.issued_dt is not null 
		 and v_control_start_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		 where control_dt = v_control_start_dt - interval '1 day'
		 and comm.subgroup_desc = 'II-Prepaid commission';
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP09 % : % row(s) - IV-Prepaid commission of previous month',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP09 AGENT-V10-IV-Prepaid commission of previous month: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;    

	--=============== Prepare V5 = II , Jan = II-III
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_commission   
		select  t.ref_1,t.nac_rk,t.natrn_x_dim_rk,t.branch_cd
		,t.sap_doc_type,t.reference_header,t.post_dt
		,t.doc_dt,t.doc_type,t.doc_no,t.accode,t.dc_flg,t.actype,t.posting_amt
		,t.system_type,t.transaction_type,t.premium_type,t.policy_no,t.plan_cd,t.rider_cd
		,t.pay_dt,t.pay_by_channel,t.sum_natrn_amt,t.posting_sap_amt,t.posting_proxy_amt
		,t.sales_id,t.sales_struct_n,t.selling_partner,t.distribution_mode
		,t.product_group,t.product_sub_group,t.rider_group,t.product_term,t.cost_center
		,t.nac_dim_txt,t.source_filename,t.group_nm,t.subgroup_nm,t.subgroup_desc
		,vn.variable_cd,vn.variable_name as variable_nm
		,t.nadet_detail,t.org_branch_cd,t.org_submit_no,t.rp_no,t.submit_no
		,t.section_order_no,t.is_section_order_no,t.for_branch_cd
		,t.event_type,t.comm_gmm_flg,t.comm_gmm_desc
		,t.policy_type,t.effective_dt,t.issued_dt
		,t.ifrs17_channelcode,t.ifrs17_partnercode,t.ifrs17_portfoliocode
		,t.ifrs17_portid,t.ifrs17_portgroup,t.is_duplicate_variable  
		from stag_a.stga_ifrs_nac_txn_step05_commission t  
		left join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V5'  
		and t.event_type  = vn.event_type
		and t.comm_gmm_flg  = vn.comm_gmm_flg   )			 	
		where t.variable_cd = 'II'; 
								
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP09 Prepare V5 II : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP09 % : % row(s) - Prepare V5 II ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
 
	if  date_part('month',v_control_start_dt) = 1 then 
		begin
			insert into stag_a.stga_ifrs_nac_txn_step05_commission 
			select  t.ref_1,t.nac_rk,t.natrn_x_dim_rk,t.branch_cd
			,t.sap_doc_type,t.reference_header,t.post_dt
			,t.doc_dt,t.doc_type,t.doc_no,t.accode,t.dc_flg,t.actype
			,t.posting_amt*-1 as posting_amt
			,t.system_type,t.transaction_type,t.premium_type,t.policy_no,t.plan_cd,t.rider_cd
			,t.pay_dt,t.pay_by_channel,t.sum_natrn_amt,t.posting_sap_amt,t.posting_proxy_amt
			,t.sales_id,t.sales_struct_n,t.selling_partner,t.distribution_mode
			,t.product_group,t.product_sub_group,t.rider_group,t.product_term,t.cost_center
			,t.nac_dim_txt,t.source_filename,t.group_nm,t.subgroup_nm,t.subgroup_desc
			,vn.variable_cd,vn.variable_name as variable_nm
			,t.nadet_detail,t.org_branch_cd,t.org_submit_no,t.rp_no,t.submit_no
			,t.section_order_no,t.is_section_order_no,t.for_branch_cd
			,t.event_type,t.comm_gmm_flg,t.comm_gmm_desc
			,t.policy_type,t.effective_dt,t.issued_dt
			,t.ifrs17_channelcode,t.ifrs17_partnercode,t.ifrs17_portfoliocode
			,t.ifrs17_portid,t.ifrs17_portgroup,t.is_duplicate_variable   
			from stag_a.stga_ifrs_nac_txn_step05_commission t  
			left join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Commission'
			and vn.variable_cd = 'V5'  
			and t.event_type  = vn.event_type
			and t.comm_gmm_flg  = vn.comm_gmm_flg )			 	
			where t.variable_cd = 'III';
	
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP09 Prepare V5 III : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;  
				
		END;  
		raise notice 'STEP10 % : % row(s) - Prepare V5 III ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	end if;

	-- 08Mar2023 Narumol W.: ยกเลิก OVDEBT ใน V5 ไปรวมใน V10 
/*
	--=========	V5 natrn-void debt - ยกเลิกตั้งหนี้เรียกคืนค่าบำเหน็จและผลประโยชน์ฝ่ายขาย ยังไม่มี กธ	
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_commission	
		select  natrn.ref_1 as ref_1 , debt.void_acctrans_rk , debt.natrn_x_dim_rk 
		,debt.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,debt.doc_dt,debt.doc_type,debt.doc_no,debt.accode,debt.dc_flg,null::varchar as actype 
		,debt.ovdebt_posting_amt  as posting_amt -- posting_sum_nac_amt
		,null::varchar system_type,debt.income_type as transaction_type,debt.premium_type
		,debt.policy_no,debt.plan_cd, debt.rider_cd  ,debt.ovdebt_dt pay_dt ,null::varchar pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,debt.sales_id,debt.sales_struct_n,debt.selling_partner,debt.distribution_mode,debt.product_group,debt.product_sub_group,debt.rider_group,debt.product_term,debt.cost_center
		,debt.acctrans_dim_txt as nac_dim_txt   
		,debt.source_filename  , 'natrn-void debt' as  group_nm  , 'V5 Void Debt' as subgroup_nm , 'ยกเลิกตั้งหนี้เรียกคืนค่าบำเหน็จและผลประโยชน์ฝ่ายขาย'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, debt.branch_cd org_branch_cd, debt.submit_no as org_submit_no  
		, case when dds.fn_is_numeric(debt.submit_no) is true and debt.submit_no <> '000000000000' then debt.submit_no else null END submit_no
		,''::varchar as receipt_no 
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then debt.submit_no else null END section_order_no
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then 1 else 0 END is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup,vn.is_duplicate_variable    
		from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_ovdebt_void_acctrans_chg debt  
		on (  natrn.doc_dt = debt.doc_dt 
		and natrn.doc_type = debt.doc_type 
		and natrn.doc_no = debt.doc_no 
		and natrn.accode = debt.accode 
		and natrn.natrn_dim_txt  = debt.acctrans_dim_txt  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( debt.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( debt.policy_no = pol.policy_no
		and  debt.plan_cd = pol.plan_cd 
		and debt.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V5' 
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg = vn.comm_gmm_flg    
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )         
		where pol.effective_dt is  null 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP10 void debt p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10 % : % row(s) - void debt V5',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

	--=========	V5 ovdebt - คืน Commission:ปฎิเสธไม่รับประกัน

	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_commission			
		select  natrn.ref_1 as ref_1 , debt.acctrans_rk , debt.natrn_x_dim_rk 
		,debt.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,debt.doc_dt,debt.doc_type,debt.doc_no,debt.accode,debt.dc_flg,null::varchar as actype 
		,debt.ovdebt_posting_amt  as posting_amt -- posting_sum_nac_amt
		,null::varchar system_type,debt.income_type as transaction_type,debt.premium_type
		,debt.policy_no,debt.plan_cd, debt.rider_cd  ,debt.ovdebt_dt pay_dt ,null::varchar pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,debt.sales_id,debt.sales_struct_n,debt.selling_partner,debt.distribution_mode,debt.product_group,debt.product_sub_group,debt.rider_group,debt.product_term,debt.cost_center
		,debt.acctrans_dim_txt as nac_dim_txt   
		, debt.source_filename  , 'natrn - debt' as  group_nm  , 'V5 Refund commission' as subgroup_nm , 'คืน Commission:ปฎิเสธไม่รับประกัน'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, debt.branch_cd org_branch_cd, debt.submit_no as org_submit_no
		, case when dds.fn_is_numeric(debt.submit_no) is true and debt.submit_no <> '000000000000' then debt.submit_no else null END submit_no
		, null::varchar as receipt_no
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then debt.submit_no else null END section_order_no
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then 1 else 0 END is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup,vn.is_duplicate_variable    
		from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_ovdebt_acctrans_chg debt  
		on (  natrn.doc_dt = debt.doc_dt 
		and natrn.doc_type = debt.doc_type 
		and natrn.doc_no = debt.doc_no 
		and natrn.accode = debt.accode 
		and natrn.natrn_dim_txt  = debt.acctrans_dim_txt  ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( debt.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( debt.policy_no = pol.policy_no
		and debt.plan_cd = pol.plan_cd
		and debt.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V5' 
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg = vn.comm_gmm_flg  )  
		--where coalesce(natrn.is_reverse,0) <> 1  
		where pol.effective_dt is null 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP10  debt p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10 % : % row(s) - debt V5',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
*/

	-- 08Mar2023 Narumol W.: ยกเลิก OVDEBT ใน V5 ไปรวมใน V10 
 	--======================== V10 natrn-void debt - ยกเลิกตั้งหนี้เรียกคืนค่าบำเหน็จและผลประโยชน์ฝ่ายขาย ยังไม่มี กธ	
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_commission	
		select  natrn.ref_1 as ref_1 , debt.void_acctrans_rk , debt.natrn_x_dim_rk 
		,debt.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,debt.doc_dt,debt.doc_type,debt.doc_no,debt.accode,debt.dc_flg,null::varchar as actype 
		,debt.ovdebt_posting_amt  as posting_amt -- posting_sum_nac_amt
		,null::varchar system_type,debt.income_type as transaction_type,debt.premium_type
		,debt.policy_no,debt.plan_cd, debt.rider_cd  ,debt.ovdebt_dt pay_dt ,null::varchar pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,debt.sales_id,debt.sales_struct_n,debt.selling_partner,debt.distribution_mode,debt.product_group,debt.product_sub_group,debt.rider_group,debt.product_term,debt.cost_center
		,debt.acctrans_dim_txt as nac_dim_txt   
		,debt.source_filename  , 'natrn-void debt' as  group_nm  , 'V10 Void Debt' as subgroup_nm , 'ยกเลิกตั้งหนี้เรียกคืนค่าบำเหน็จและผลประโยชน์ฝ่ายขาย'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, debt.branch_cd org_branch_cd, debt.submit_no as org_submit_no  
		, case when dds.fn_is_numeric(debt.submit_no) is true and debt.submit_no <> '000000000000' then debt.submit_no else null END submit_no
		,''::varchar as receipt_no 
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then debt.submit_no else null END section_order_no
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then 1 else 0 END is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup,vn.is_duplicate_variable    
		from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_ovdebt_void_acctrans_chg debt  
		on (  natrn.natrn_x_dim_rk  = debt.natrn_x_dim_rk ) 
		/*on (  natrn.doc_dt = debt.doc_dt 
		and natrn.doc_type = debt.doc_type 
		and natrn.doc_no = debt.doc_no 
		and natrn.accode = debt.accode 
		-- 20Mar2023 Narumol W : Commission Log 352Commission Log 352 Add Key join sales_id
		and natrn.sales_id = debt.sales_id 
		and natrn.natrn_dim_txt  = debt.acctrans_dim_txt  )  */
		left join  stag_s.stg_tb_core_planspec pp 
		on ( debt.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( debt.policy_no = pol.policy_no
		and  debt.plan_cd = pol.plan_cd 
		and debt.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V10' 
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg = vn.comm_gmm_flg    
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )         
		-- 08Mar2023 Narumol W.: ยกเลิก OVDEBT ใน V5 ไปรวมใน V10 
		--where pol.effective_dt is not null 
		where natrn.post_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP10 void debt p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10.1 % : % row(s) - void debt V10',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

	--=========	V10 ovdebt - คืน Commission:ปฎิเสธไม่รับประกัน 
	begin
		insert into stag_a.stga_ifrs_nac_txn_step05_commission			
		select  natrn.ref_1 as ref_1 , debt.acctrans_rk , debt.natrn_x_dim_rk 
		,debt.branch_cd
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,debt.doc_dt,debt.doc_type,debt.doc_no,debt.accode,debt.dc_flg,null::varchar as actype 
		,debt.ovdebt_posting_amt  as posting_amt -- posting_sum_nac_amt
		,null::varchar system_type,debt.income_type as transaction_type,debt.premium_type
		,debt.policy_no,debt.plan_cd, debt.rider_cd  ,debt.ovdebt_dt pay_dt ,null::varchar pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,debt.sales_id,debt.sales_struct_n,debt.selling_partner,debt.distribution_mode,debt.product_group,debt.product_sub_group,debt.rider_group,debt.product_term,debt.cost_center
		,debt.acctrans_dim_txt as nac_dim_txt   
		, debt.source_filename  , 'natrn - debt' as  group_nm  , 'V10 Refund commission' as subgroup_nm , 'คืน Commission:ปฎิเสธไม่รับประกัน'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, debt.branch_cd org_branch_cd, debt.submit_no as org_submit_no
		, case when dds.fn_is_numeric(debt.submit_no) is true and debt.submit_no <> '000000000000' then debt.submit_no else null END submit_no
		, null::varchar as receipt_no
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then debt.submit_no else null END section_order_no
		, case when dds.fn_is_numeric(debt.submit_no) is false and dds.fn_is_numeric(substring(debt.submit_no,5,1)) is false then 1 else 0 END is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup,vn.is_duplicate_variable    
		from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_ovdebt_acctrans_chg debt  
		on (  natrn.natrn_x_dim_rk  = debt.natrn_x_dim_rk ) 
		/*on (  natrn.doc_dt = debt.doc_dt 
		and natrn.doc_type = debt.doc_type 
		and natrn.doc_no = debt.doc_no 
		and natrn.accode = debt.accode 
		-- 20Mar2023 Narumol W : Commission Log 352Commission Log 352 Add Key join sales_id
		and natrn.sales_id = debt.sales_id 
		and natrn.natrn_dim_txt  = debt.acctrans_dim_txt  ) */
		left join  stag_s.stg_tb_core_planspec pp 
		on ( debt.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( debt.policy_no = pol.policy_no
		and debt.plan_cd = pol.plan_cd
		and debt.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V10' 
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg = vn.comm_gmm_flg
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )      
		--where coalesce(natrn.is_reverse,0) <> 1  
		-- 08Mar2023 Narumol W.: ยกเลิก OVDEBT ใน V5 ไปรวมใน V10 
		--where pol.effective_dt is not null 
		where natrn.post_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP10  debt p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10.2 % : % row(s) - debt V10',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	-- 23May2023 Narumol W. ITRQ-66052187 ยกเลิก V10-Suspense 
	/*
	--=========	V10 ovdebt - คืน Commission:ปฎิเสธไม่รับประกัน 
	begin
		
		insert into stag_a.stga_ifrs_nac_txn_step05_commission			
		select  natrn.ref_1 as ref_1 ,sus.suspense_newcase_rk::bigint as nac_rk , natrn.natrn_x_dim_rk as natrn_x_dim_rk  
		,sus.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,sus.doc_dt,natrn.doc_type,sus.doc_no,natrn.accode,natrn.dc_flg,''::varchar as actype  
		,sus.posting_suspense_amt 
		 ,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,case when sus.policy_no = '00000000' then sus.x_policy_no else sus.policy_no END as policy_no
		,sus.plan_cd,sus.rider_cd as rider_cd ,sus.pay_dt ,null::varchar as pay_by_channel 
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,sus.sales_id,sus.sales_struct_n,sus.selling_partner,sus.distribution_mode,sus.product_group,sus.product_sub_group,sus.rider_group,sus.product_term,sus.cost_center
		,suspense_dim_txt
		, sus.filename as source_filename  , 'natrn-suspense' as  group_nm  , 'V9 Suspense new case' as subgroup_nm , 'กลับรายการเบิกพักเบี้ยฯ และตั้งค่าบำเหน็จค้างจ่าย'  as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, sus.branch_cd as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no, ''::varchar as rp_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no 
		,sus.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable    
		from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )
		inner join dds.ifrs_suspense_newcase_chg sus   
		on (  natrn.for_branch_cd = sus.for_branch_cd
		and natrn.doc_dt = sus.doc_dt  
		and natrn.doc_type = 'OTH'
		and natrn.doc_no = sus.doc_no 
		and natrn.accode = sus.c_accode 
		and natrn.natrn_dim_txt = sus.suspense_dim_txt )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( sus.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( sus.x_policy_no = pol.policy_no
		and sus.plan_cd = pol.plan_cd 
		and sus.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission' 
		and vn.variable_cd = 'V10'
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg  = vn.comm_gmm_flg   
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )        
		where natrn.post_dt between v_control_start_dt and v_control_end_dt; 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP10 Suspense newcase p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10.3 % : % row(s) - Suspense newcase V10',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	*/
	--=========	V10 payment_trans - จ่ายเงินเวนคืน CT Auto
	begin
        -- 22Mar2023 Narumol W.: Chg.87 add source payment_trans for CT_AUTO
        insert into stag_a.stga_ifrs_nac_txn_step05_commission
		select natrn.ref_1 as ref_1 ,refund.payment_transpos_rk::bigint as nac_rk,  natrn.natrn_x_dim_rk 
		, refund.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, refund.doc_dt , refund.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, refund.posting_refund_amt   as posting_amt
		, refund.system_type ,refund.transaction_type,refund.premium_type 
		, refund.policy_no as policy_no ,coalesce (pol.plan_cd ,refund.plan_cd) as plan_cd , refund.rider_cd as rider_cd , null::date as pay_dt ,null::varchar as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, refund.sales_id,refund.sales_struct_n,refund.selling_partner,refund.distribution_mode,refund.product_group,refund.product_sub_group,refund.rider_group,refund.product_term,refund.cost_center
		, refund.transpos_dim_txt as refund_dim_txt  
		, 'payment_trans'::varchar as source_filename  , 'natrn-payment_trans' as  group_nm  , 'CT AUTO' as subgroup_nm ,'จ่ายเงินเวนคืน CT Auto'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, refund.branch_cd as org_branch_cd 
		, null::varchar as org_submit_no  
		, case when trim(refund.rp_no) in ( '','00000000000000000000') then null::varchar else  trim(rp_no) end as rp_no 
		, case when trim(refund.submit_no) in ( '','00000000000000000000') then null::varchar else  trim(submit_no) end as submit_no  
		, null::varchar as section_order_no ,0 as is_section_order_no
		,refund.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,coalesce (refund.policy_type,pol.policy_type) as policy_type 
		,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
		inner join dds.ifrs_payment_transpos_chg  refund  
		on ( natrn.org_branch_cd =  refund.ref_branch_cd   
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
		 and refund.plan_cd = pol.plan_cd  
		 and natrn.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Commission'
		 and vn.variable_cd = 'V10'  
		 and natrn.comm_gmm_flg  = vn.comm_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where natrn.post_dt  between  v_control_start_dt and v_control_end_dt
        and refund.system_type = 'CT_AUTO'; 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP10 Payment_trans p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10.4 % : % row(s) - Payment_trans V10',clock_timestamp()::varchar(19),v_affected_rows::varchar;	


	--=========	V10 nac - จ่ายค่าจัดงานประจำเดือน
	begin
        -- 22Mar2023 Narumol W.: Chg.88 add source nac for จ่ายค่าจัดงานประจำเดือน
        insert into stag_a.stga_ifrs_nac_txn_step05_commission
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
		,nac.filename as source_filename  , 'natrn-nac'::varchar(100) as  group_nm , 'SALESBENEFIT_OV'::varchar(100) as subgroup_nm , 'จ่ายค่าจัดงานประจำเดือน' ::varchar(100) as subgroup_desc  
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		, nac.org_branch_cd::varchar(10) as org_branch_cd
		, nac.submit_no as org_submit_no
		, case when trim(nac.receipt_no) in ( '','00000000000000000000') then null::varchar else  trim(nac.receipt_no) end as rp_no  
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is true and nac.submit_no <> '000000000000' then nac.submit_no else null end submit_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then nac.submit_no else null end section_order_no
		, case when dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false and dds.fn_is_numeric(substring(nac.submit_no,5,1)) is false then 1 else 0 end is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup , vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04   as natrn 
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
		 on ( vn.event_type = 'Commission'
		 and vn.variable_cd = 'V10'
		 and vn.comm_gmm_flg  = natrn.comm_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ))	
		where nac.system_type  = 'SALESBENEFIT_OV'
		and  dds.fn_is_numeric(coalesce(nac.submit_no,'0')) is false 
		and natrn.post_dt  between v_control_start_dt and v_control_end_dt;		 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP10 Payment_trans p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10.5 % : % row(s) - NAC-SALESBENEFIT_OV V10',clock_timestamp()::varchar(19),v_affected_rows::varchar;	

	--=============== Prepare V10 = I-II + IV (+ III)
	begin
		
		update stag_a.stga_ifrs_nac_txn_step05_commission 
		set variable_cd ='V10'
		where variable_cd ='I';
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP11 % : % row(s) - Prepare V10 III ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

		update stag_a.stga_ifrs_nac_txn_step05_commission 
		set variable_cd ='V10'
		,posting_amt = posting_amt*-1 
		where variable_cd ='II';
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP13 % : % row(s) - Prepare V10 II',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

		update stag_a.stga_ifrs_nac_txn_step05_commission 
		set variable_cd ='V10' 
		where variable_cd ='IV';
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP14 % : % row(s) - Prepare V10 IV ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
  
		if  date_part('month',v_control_start_dt) = 1 then 
			update stag_a.stga_ifrs_nac_txn_step05_commission 
			set variable_cd ='V10'
			,posting_amt = posting_amt 
			where variable_cd ='III';
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP12 % : % row(s) - Prepare V10 III ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
		end if;
	
 
		-- 03Mar2023 Narumol W. : Log.333 Update variable_nm 
		UPDATE stag_a.stga_ifrs_nac_txn_step05_commission 
		SET variable_nm = vn.variable_name   
		FROM stag_s.stg_tb_core_planspec pp 
		,stag_s.ifrs_common_variable_nm vn 
		where stag_a.stga_ifrs_nac_txn_step05_commission.plan_cd = pp.plan_cd  
		and vn.event_type = 'Commission'
		and vn.variable_cd = 'V10' 
		and stag_a.stga_ifrs_nac_txn_step05_commission.event_type  = vn.event_type
		and stag_a.stga_ifrs_nac_txn_step05_commission.comm_gmm_flg = vn.comm_gmm_flg
		and stag_a.stga_ifrs_nac_txn_step05_commission.variable_cd = vn.variable_cd
		and ( vn.ifrs17_var_future = pp.ifrs17_var_future   or vn.ifrs17_var_current  = pp.ifrs17_var_current );
	
	 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP15 % : % row(s) - Update variable_nm Prepare V10 III ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP10 Prepare V10 III : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;   
 
  	--============================================================================================================================--

	---------------------- AGENT - V11 Accrued 
	BEGIN
		insert into  stag_a.stga_ifrs_nac_txn_step05_commission 
		select  accru.ref_1 as ref_1,accru.accrual_rk as nac_rk,  natrn.natrn_x_dim_rk -- , natrn.doc_type 
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd 
		, comm.pay_dt as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  
		, 'Accruedac_current' as subgroup_nm , 'V11 บันทึกค่า commission กับ ค้างจ่าย ณ สิ้นเดือน (Monthly)'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , accru.rp_no,''::varchar as submit_no 
		, ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
		from  dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn 
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no  
		and natrn.dc_flg = accru.dc_flg
		and natrn.accode = accru.accode
		and natrn.natrn_dim_txt =  accru.accru_dim_txt )
		left outer join dds.ifrs_agent_comm_chg  comm    
		on ( accru.submit_no = comm.submit_no 
		and accru.rp_no = comm.rpno 
		and accru.policy_no = comm.policy_no
		and comm.rider_cd <> '_OTH') 
		left join  stag_s.stg_tb_core_planspec pp 
		on (accru.plan_code = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( accru.policy_no = pol.policy_no
		and accru.plan_code = pol.plan_cd 
		and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		 and vn.variable_cd = 'V11' 
		 and natrn.event_type  = vn.event_type
		 and natrn.comm_gmm_flg  = vn.comm_gmm_flg 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
		--where coalesce(natrn.is_reverse,0) <> 1    
		where accru.doc_dt  between v_control_start_dt and v_control_end_dt 
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_commission  bb 
						where bb.nac_rk = accru.accrual_rk 
						and bb.accode = accru.accode
						and source_filename = 'accrued-ac'); 
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP14 AGENT-V11-Accrued commission : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP14 % : % row(s) - AGENT-V11-Accrued commission',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- 23May2023 Narumol W. ITRQ-66052187 ขอเพิ่ม source suspense newcase ใน V11 V12 ของ COM_YTD
	begin
		
		if date_part('month',v_control_start_dt) = 12 then 
			insert into stag_a.stga_ifrs_nac_txn_step05_commission			
			select  natrn.ref_1 as ref_1 ,sus.suspense_newcase_rk::bigint as nac_rk , natrn.natrn_x_dim_rk as natrn_x_dim_rk  
			-- 31Jul2023 Narumol W.: Commission Log.362 : Use ntrn.branch_cd instead of sus.branch_cd 
			,natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
			,sus.doc_dt,natrn.doc_type,sus.doc_no,natrn.accode,natrn.dc_flg,''::varchar as actype  
			,sus.posting_suspense_amt 
			 ,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
			,case when sus.policy_no = '00000000' then sus.x_policy_no else sus.policy_no END as policy_no
			,sus.plan_cd,sus.rider_cd as rider_cd ,sus.pay_dt ,null::varchar as pay_by_channel 
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,sus.sales_id,sus.sales_struct_n,sus.selling_partner,sus.distribution_mode,sus.product_group,sus.product_sub_group,sus.rider_group,sus.product_term,sus.cost_center
			,suspense_dim_txt
			, sus.filename as source_filename  , 'natrn-suspense' as  group_nm  
			, 'V11 Suspense new case' as subgroup_nm , 'V11 บันทึกค่า commission กับ ค้างจ่าย ณ สิ้นปี (Yearly)'  as subgroup_desc  
			, vn.variable_cd
			, vn.variable_name as variable_nm 
			, natrn.detail as nadet_detail 
			, sus.branch_cd as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no, ''::varchar as rp_no 
			, ''::varchar as section_order_no ,0 as is_section_order_no 
			,sus.for_branch_cd
			,natrn.event_type 
			,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable    
			from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )
			inner join dds.ifrs_suspense_newcase_chg sus   
			on (  natrn.for_branch_cd = sus.for_branch_cd
			and natrn.doc_dt = sus.doc_dt  
			and natrn.doc_type = 'OTH'
			and natrn.doc_no = sus.doc_no 
			and natrn.accode = sus.c_accode 
			and natrn.natrn_dim_txt = sus.suspense_dim_txt )
			left join  stag_s.stg_tb_core_planspec pp 
			on ( sus.plan_cd = pp.plan_cd) 
			left outer join dds.tl_acc_policy_chg pol  
			on ( sus.x_policy_no = pol.policy_no
			and sus.plan_cd = pol.plan_cd 
			and sus.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
			inner join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Commission' 
			and vn.variable_cd = 'V11'
			and natrn.event_type  = vn.event_type
			and natrn.comm_gmm_flg  = vn.comm_gmm_flg   
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )      
			where sus.is_cut_off ='0'
			and coalesce( sus.effective_dt,sus.end_month_dt ) <= sus.end_month_dt
			and sus.policy_no <> '00000000'
			and sus.application_status_cd = 'M' 
			and natrn.post_dt between v_control_start_dt and v_control_end_dt;		
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP16 % : % row(s) - V11 DECEMBER Suspense new case ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	
		else 
			insert into stag_a.stga_ifrs_nac_txn_step05_commission			
			select  natrn.ref_1 as ref_1 ,sus.suspense_newcase_rk::bigint as nac_rk , natrn.natrn_x_dim_rk as natrn_x_dim_rk  
			-- 31Jul2023 Narumol W.: Commission Log.362 : Use ntrn.branch_cd instead of sus.branch_cd 
			,natrn.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
			,sus.doc_dt,natrn.doc_type,sus.doc_no,natrn.accode,natrn.dc_flg,''::varchar as actype  
			,sus.posting_suspense_amt 
			 ,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
			,case when sus.policy_no = '00000000' then sus.x_policy_no else sus.policy_no END as policy_no
			,sus.plan_cd,sus.rider_cd as rider_cd ,sus.pay_dt ,null::varchar as pay_by_channel 
			,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,sus.sales_id,sus.sales_struct_n,sus.selling_partner,sus.distribution_mode,sus.product_group,sus.product_sub_group,sus.rider_group,sus.product_term,sus.cost_center
			,suspense_dim_txt
			, sus.filename as source_filename  , 'natrn-suspense' as  group_nm  
			, 'Accruedac_current' as subgroup_nm , 'V11 บันทึกค่า commission กับ ค้างจ่าย ณ สิ้นเดือน (Monthly)'  as subgroup_desc  
			, vn.variable_cd
			, vn.variable_name as variable_nm 
			, natrn.detail as nadet_detail 
			, sus.branch_cd as org_branch_cd  , ''::varchar as org_submit_no , ''::varchar as submit_no, ''::varchar as rp_no 
			, ''::varchar as section_order_no ,0 as is_section_order_no 
			,sus.for_branch_cd
			,natrn.event_type 
			,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable    
			from  stag_a.stga_ifrs_nac_txn_step04  as natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )
			inner join dds.ifrs_suspense_newcase_chg sus   
			on (  natrn.for_branch_cd = sus.for_branch_cd
			and natrn.doc_dt = sus.doc_dt  
			and natrn.doc_type = 'OTH'
			and natrn.doc_no = sus.doc_no 
			and natrn.accode = sus.c_accode 
			and natrn.natrn_dim_txt = sus.suspense_dim_txt )
			left join  stag_s.stg_tb_core_planspec pp 
			on ( sus.plan_cd = pp.plan_cd) 
			left outer join dds.tl_acc_policy_chg pol  
			on ( sus.x_policy_no = pol.policy_no
			and sus.plan_cd = pol.plan_cd 
			and sus.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
			inner join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Commission' 
			and vn.variable_cd = 'V11'
			and natrn.event_type  = vn.event_type
			and natrn.comm_gmm_flg  = vn.comm_gmm_flg   
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )        
			where sus.is_cut_off ='0'
			and coalesce( sus.mst_effective_dt ,sus.end_month_dt ) <= sus.end_month_dt
			and coalesce( sus.pay_dt,sus.end_month_dt ) <= sus.end_month_dt
			and coalesce( sus.account_dt,sus.end_month_dt ) <= sus.end_month_dt
			and coalesce( sus.nb_effective_dt,sus.end_month_dt ) <= sus.end_month_dt
			and coalesce( sus.effective_dt,sus.end_month_dt ) <= sus.end_month_dt
			and coalesce( sus.policy_dt,sus.end_month_dt ) <= sus.end_month_dt
			and sus.policy_no <> '00000000'
			and sus.application_status_cd = 'M' 
			and natrn.post_dt between v_control_start_dt and v_control_end_dt;
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP17 % : % row(s) - V11 Suspense new case ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		
		end if; 
	
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			EXCEPTION 
			WHEN OTHERS THEN 
				out_err_cd := 1;
				out_err_msg := SQLERRM;
				INSERT INTO dds.tb_fn_log
				VALUES ( 'dds.ifrs_variable_txn05_commission' 				
					,'ERROR STEP10 Suspense newcase p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
				,SQLSTATE,SQLERRM,clock_timestamp());		
				 RETURN out_err_cd ;
				
	END;  
	raise notice 'STEP10.3 % : % row(s) - Suspense newcase V10',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	---------------------- AGENT - V12 Accrued last year
	begin
		-- log.293 เอาใบสำคัญที่ตั้ง accrued เดือน ธ.ค.ปีที่แล้ว ส่งมากลับ sign ในเดือนมกราคมปีปัจจุบัน ครั้งเดียว เดือนอื่นไม่ต้องส่งใช้วิธี Append
		if date_part('month',v_control_start_dt) = 1  then
			
	        	drop table stag_a.temp_accru;		

			create table  stag_a.temp_accru as 	       
			with stga_ifrs_nac_txn_step04_com as (
			select * from stag_a.stga_ifrs_nac_txn_step04 where event_type  = 'Commission' ) 	
			select  accru.ref_1  ,accru.accrual_rk  ,  natrn.natrn_x_dim_rk -- , accru.doc_type 
			, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header 
			, accru.doc_dt,accru.doc_type,accru.doc_no
			, accru.accode,accru.dc_flg 
			, accru.ac_type 
			, accru.posting_accru_amount 
			, accru.system_type_cd  ,accru.transaction_type,accru.premium_type 
			, accru.policy_no ,accru.plan_code as plan_cd  , accru.rider_code    ,accru.pay_by  
			, posting_natrn_amt   ,posting_sap_amt,posting_proxy_amt   
			, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode
			, accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
			, accru.accru_dim_txt
			, natrn.detail   ,accru.org_branch_cd
			, natrn.for_branch_cd
			, natrn.event_type 
			, natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
			, accru.submit_no , accru.rp_no 			
			from  dds.vw_ifrs_accrual_eoy_chg accru  
			inner join stga_ifrs_nac_txn_step04_com natrn 
			on ( natrn.branch_cd = accru.branch_cd
			and natrn.doc_dt = accru.doc_dt
			and natrn.doc_type = accru.doc_type
			and natrn.doc_no = accru.doc_no  
			and natrn.dc_flg = accru.dc_flg
			and natrn.accode = accru.accode
			and natrn.natrn_dim_txt =  accru.accru_dim_txt );
				
		    insert into stag_a.stga_ifrs_nac_txn_step05_commission 
			select  accru.ref_1 as ref_1,accru.accrual_rk as nac_rk,  accru.natrn_x_dim_rk -- , accru.doc_type 
			, accru.branch_cd ,accru.sap_doc_type,accru.reference_header,'2025-01-31' as post_dt --accru.post_dt
			, accru.doc_dt,accru.doc_type,accru.doc_no
			, accru.accode,accru.dc_flg 
			, accru.ac_type 
			, accru.posting_accru_amount*-1 as posting_amt
			, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
			, accru.policy_no ,accru.plan_cd, accru.rider_code as rider_cd ,comm.pay_dt as pay_dt ,accru.pay_by as pay_by_channel
			, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
			, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
			, accru.accru_dim_txt
			, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'V12 Accrued Ac' as subgroup_nm , 'ล้าง Commission & ค้างจ่าย ที่ตั้งไว้ของงวดก่อน'  as subgroup_desc 
			, vn.variable_cd
			, vn.variable_name as variable_nm  
			, accru.detail as nadet_detail ,accru.org_branch_cd
			, ''::varchar as org_submit_no  ,accru.rp_no , ''::varchar as submit_no
			, ''::varchar as section_order_no ,0 as is_section_order_no
			, accru.for_branch_cd
			, accru.event_type 
			, accru.comm_gmm_flg ,accru.comm_gmm_desc 
			, pol.policy_type ,pol.effective_dt ,pol.issued_dt
			, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			, pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
			from  stag_a.temp_accru accru
			left join dds.ifrs_agent_comm_chg  comm    
			on ( accru.submit_no = comm.submit_no 
			and accru.rp_no = comm.rpno 
			and accru.policy_no = comm.policy_no
			and comm.rider_cd <> '_OTH') 
			left join  stag_s.stg_tb_core_planspec pp 
			on (accru.plan_cd = pp.plan_cd) 
			left join dds.tl_acc_policy_chg pol  
			on ( accru.policy_no = pol.policy_no
			and accru.plan_cd = pol.plan_cd 
			and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
			inner join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Commission'
			and vn.variable_cd = 'V12' 
			and accru.event_type  = vn.event_type
			and accru.comm_gmm_flg  = vn.comm_gmm_flg 
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) );		
			
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
		end if;
	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP15 AGENT-V12-Accrued commission END of last year : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP15 % : % row(s) - AGENT-V12-Accrued commission END of last year ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	begin
		-- 16Jun2023 Narumol W. ITRQ-66052187 ขอเพิ่ม source suspense newcase ใน V11 V12 ของ COM_YTD           
		if date_part('month',v_control_start_dt) = 1 then 
		 
			insert into  stag_a.stga_ifrs_nac_txn_step05_commission 
			select v.ref_1,v.nac_rk,v.natrn_x_dim_rk,v.branch_cd,v.sap_doc_type,v.reference_header
			--,'2023-01-31' as post_dt
			,v_control_end_dt as post_dt
			,v.doc_dt,v.doc_type,v.doc_no,v.accode,v.dc_flg,v.actype
			,v.posting_amt*-1 as posting_amt
			,v.system_type,v.transaction_type,v.premium_type,v.policy_no,v.plan_cd,v.rider_cd,v.pay_dt,v.pay_by_channel
			,v.sum_natrn_amt,v.posting_sap_amt,v.posting_proxy_amt
			,v.sales_id,v.sales_struct_n,v.selling_partner,v.distribution_mode,v.product_group
			,v.product_sub_group,v.rider_group,v.product_term,v.cost_center,v.nac_dim_txt
			,vn.variable_cd,vn.variable_name 
			,'V12 Suspense new case' subgroup_nm,v.subgroup_desc
			,vn.variable_cd,vn.variable_name 
			,v.nadet_detail
			,v.org_branch_cd,v.org_submit_no,v.submit_no,v.rp_no,v.section_order_no,v.is_section_order_no
			,v.for_branch_cd,v.event_type,v.comm_gmm_flg,v.comm_gmm_desc,v.policy_type,v.effective_dt
			,v.issued_dt,v.ifrs17_channelcode,v.ifrs17_partnercode,v.ifrs17_portfoliocode,v.ifrs17_portid
			,v.ifrs17_portgroup,v.is_duplicate_variable 
			from dds.ifrs_variable_commission_ytd v 
			left join  stag_s.stg_tb_core_planspec pp 
			on (v.plan_cd = pp.plan_cd) 
			inner join stag_s.ifrs_common_variable_nm vn 
			 on ( vn.event_type = 'Commission'
			 and vn.variable_cd = 'V12' 
			 and v.event_type  = vn.event_type
			 and v.comm_gmm_flg  = vn.comm_gmm_flg 
			 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) ) 
			where control_dt =  v_end_eoy_dt
			and subgroup_nm = 'V11 Suspense new case'; 
		
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
			raise notice 'STEP15 % : % row(s) - V12 DECEMBER Suspense new case ',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
		
		end if;
	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP15 AGENT-V12-Accrued commission END of last year : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP15 % : % row(s) - AGENT-V12-Accrued commission END of last year ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	--============================================= V13 sap_trial_balance
	-- Oil 20230516 : Add V13 stag_f.tb_sap_trial_balance
 	-- 14Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
     BEGIN  
	     
   		insert into stag_a.stga_ifrs_nac_txn_step05_commission
        (post_dt,doc_dt,accode,posting_amt
        ,event_type,source_filename,group_nm,subgroup_nm,subgroup_desc
        ,variable_cd,variable_nm,comm_gmm_flg,comm_gmm_desc)
         select report_date as post_dt,report_date as doc_dt 
		, trn.gl_acct as accode 
		, trn.accumulated_balance*-1 as posting_amt 
		, 'Commission' as event_type 
		, 'sap_trial_balance' as source_filename
		, 'SAP_TB' as group_nm 
		, 'Accruedac_current' as subgroup_nm 
		, 'V13-Non Agent เก็บยอด Outstanding จาก Account ค่าคอมจ่ายล่วงหน้า' as subgroup_desc
		, coa.dummy_variable_nm as variable_cd   
		, coalesce(vn.variable_name, coa.variable_for_policy_missing)  as variable_nm
		,coa.comm_gmm_flg ,coa.comm_gmm_desc
		from  dds.tb_sap_trial_balance trn   -- >>  dds.tb_sap_trial_balance trn
		inner join stag_s.ifrs_common_coa  coa
		on ( coa.event_type = 'TB_Commission'
		and coa.accode  = trn.gl_acct  ) 
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'TB_Commission'
        and vn.variable_cd = 'V13' 
        and coa.comm_gmm_flg   = vn.comm_gmm_flg ) 
		where trn.ledger = '0l'
		and report_date =  v_control_end_dt; 
               
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP16  : V13 - sap_trial_balance  : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP16 % : % row(s) - V13 - sap_trial_balance: Comm Expense',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  
 	-- 24May2023 Narumol W.: ITRQ-66052189 ขอเพิ่มเงื่อนไข Var.14 COM_YTD (IFRS17)
 	-- 06Feb2024 Narumol W.:[ITRQ#67010482]CR_Enhance V14 COMMISSION_YTD Data
	begin
		
		insert into  stag_a.stga_ifrs_nac_txn_step05_commission 
 		(nac_rk,post_dt,doc_dt
 		,sap_doc_type ,accode
 		,posting_amt
 		,policy_no,plan_cd,pay_dt
 		,org_submit_no,submit_no,rp_no
 		,source_filename,group_nm,subgroup_nm,subgroup_desc
 		,event_type,variable_cd,variable_nm
 		,policy_type,effective_dt,issued_dt
 		,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode 
		,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable)  
		select comm.agent_comm_rk as nac_rk
		,comm.submit_dt as post_dt , comm.submit_dt as doc_dt 
		-- 06Feb2024 Narumol W.:[ITRQ#67010482]CR_Enhance V14 COMMISSION_YTD Data
		,'SB'::varchar as sap_doc_type, '5061010010'::varchar as accode
		,comm.commission_amt*-1 as posting_comm_amt
		,comm.policy_no ,comm.plan_cd ,comm.pay_dt
		,comm.submit_no,comm.submit_no,comm.rpno
		, 'ifrs_agent_comm_chg' as source_filename 
		, 'ifrs_agent_comm_chg' as  group_nm  
		, 'ifrs_agent_comm_chg' as subgroup_nm 
		, 'V14 บันทึก Other Commission จาก Policy ที่ได้รับการอนุมัติแล้ว'  as subgroup_desc 
		, 'Commission' as event_type 
		, vn.variable_cd
		, vn.variable_name
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  ,vn.is_duplicate_variable 
		from dds.ifrs_agent_comm_chg  comm   
		left join  stag_s.stg_tb_core_planspec pp 
		on (comm.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol  
		on ( comm.policy_no = pol.policy_no
		and comm.plan_cd = pol.plan_cd 
		and comm.submit_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V14'  
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	  
		where comm.rider_cd = '_OTH'   
		and comm.submit_dt  between v_control_start_dt and v_control_end_dt; 
 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP17-V14 บันทึก Other Commission จาก Policy ที่ได้รับการอนุมัติแล้ว : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP17 % : % row(s) - V14 บันทึก Other Commission จาก Policy ที่ได้รับการอนุมัติแล้ว ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- V6 Chg.65
	-- 24Jan2023 : K.Ball ยกเลิก V6
	/*
	BEGIN	
		
		if date_part('month',v_control_start_dt) = 1  then
		
			insert into  stag_a.stga_ifrs_nac_txn_step05_commission 
			select v.ref_1,v.nac_rk,v.natrn_x_dim_rk,v.branch_cd,v.sap_doc_type,v.reference_header
			,v_control_end_dt as post_dt
			,v.doc_dt,v.doc_type,v.doc_no,v.accode,v.dc_flg,v.actype
			,v.posting_amt*-1 as posting_amt
			,v.system_type,v.transaction_type,v.premium_type,v.policy_no,v.plan_cd,v.rider_cd,v.pay_dt,v.pay_by_channel
			,v.sum_natrn_amt,v.posting_sap_amt,v.posting_proxy_amt
			,v.sales_id,v.sales_struct_n,v.selling_partner,v.distribution_mode,v.product_group
			,v.product_sub_group,v.rider_group,v.product_term,v.cost_center,v.nac_dim_txt
			,v.source_filename,v.group_nm,v.subgroup_nm,v.subgroup_desc,v.variable_cd,v.variable_nm,v.nadet_detail
			,v.org_branch_cd,v.org_submit_no,v.submit_no,v.rp_no,v.section_order_no,v.is_section_order_no
			,v.for_branch_cd,v.event_type,v.comm_gmm_flg,v.comm_gmm_desc,v.policy_type,v.effective_dt
			,v.issued_dt,v.ifrs17_channelcode,v.ifrs17_partnercode,v.ifrs17_portfoliocode,v.ifrs17_portid
			,v.ifrs17_portgroup,v.is_duplicate_variable 
			from dds.ifrs_variable_commission_ytd v
			where control_dt = v_end_eoy_dt
			and variable_cd ='V3';
			GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		END if;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP15 AGENT-V6-Accrued commission END of last year : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP15 % : % row(s) - AGENT-V6-Accrued commission END of last year ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	*/

	---------------------- AGENT - V10 Manual 
	BEGIN  
		-- STEP_01 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) <= 8	
	  	insert into  stag_a.stga_ifrs_nac_txn_step05_commission  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no,pol.plan_cd,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt    
		,natrn.filename as source_filename , 'natrn' as  group_nm  , 'V10 Manual Paid after issued' as subgroup_nm 
		, 'จ่ายค่า commission เป็นเงินสดตอนที่มีกรมธรรมแล้ว (Daily)'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as rp_no, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		--,vn.ifrs17_var_current ,pp.ifrs17_var_current , vn.ifrs17_var_future, pp.ifrs17_var_future
		from  stag_a.stga_ifrs_nac_txn_step04 natrn   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )   
		left outer join dds.tl_acc_policy_chg pol  
		on ( natrn.nadet_policyno  = pol.policy_no
		and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm
		and pol.policy_type not in ( 'M','G')) -- Oil 25jan2023 : policy_type not in ( 'M','G')
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission' 
		and vn.variable_cd = 'V10'
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg  = vn.comm_gmm_flg   	 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where  natrn.doc_no like '5%'  
		and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
		and pol.effective_dt is not null 
		and natrn.doc_dt  between v_control_start_dt and v_control_end_dt;  
	 	GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP17 % : % row(s) - AGENT-V10-Manual docno 5 STEP_01 length nadet_policyno <= 8',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	-- STEP_02 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) > 9		
		insert into  stag_a.stga_ifrs_nac_txn_step05_commission  
		select  natrn.ref_1 as ref_1 , null::bigint as nac_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no,pol.plan_cd,null::varchar as rider_cd  ,null::Date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt    
		,natrn.filename as source_filename , 'natrn' as  group_nm  , 'V10 Manual Paid after issued' as subgroup_nm 
		, 'จ่ายค่า commission เป็นเงินสดตอนที่มีกรมธรรมแล้ว (Daily)'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as rp_no, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable
		--,vn.ifrs17_var_current ,pp.ifrs17_var_current , vn.ifrs17_var_future, pp.ifrs17_var_future
		from  stag_a.stga_ifrs_nac_txn_step04 natrn   
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )   
		left outer join dds.tl_acc_policy_chg pol  
		on ( trim(natrn.nadet_policyno) = concat(trim(pol.plan_cd),trim(pol.policy_no))
		and pol.policy_type  in ('M','G','B')
		and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd)
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Commission' 
		and vn.variable_cd = 'V10'
		and natrn.event_type  = vn.event_type
		and natrn.comm_gmm_flg  = vn.comm_gmm_flg   	 
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	
		where  natrn.doc_no like '5%'  
		and length(nullif(trim(natrn.nadet_policyno),''))> 9 
		and pol.effective_dt is not null 
		and natrn.doc_dt  between v_control_start_dt and v_control_end_dt;  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP17 AGENT-V10-Manual docno 5 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;
	raise notice 'STEP17 % : % row(s) - AGENT-V10-Manual docno 5 STEP_02 length nadet_policyno > 9',clock_timestamp()::varchar(19),v_affected_rows::varchar;

/*ใช้เงื่อนไขตาม V2 แทน
	-- =================================== MANUAL from GL SAP =========================== -- 
	BEGIN	
		 
		------------- Manual  
		insert into stag_a.stga_ifrs_nac_txn_step05_commission
		(ref_1,nac_rk,branch_cd,sap_doc_type
		,reference_header,post_dt,doc_dt,doc_no
		,accode,dc_flg,posting_amt
		,policy_no,plan_cd
		,sum_natrn_amt,posting_sap_amt,posting_proxy_amt 
		,selling_partner,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center
		,nac_dim_txt,source_filename,group_nm,subgroup_nm,subgroup_desc
		,variable_cd,variable_nm,nadet_detail
		,event_type,comm_gmm_flg,comm_gmm_desc,policy_type,effective_dt,issued_dt
		,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid,ifrs17_portgroup,is_duplicate_variable)
		select sap.ref1_header
		,sap.line_item as nac_rk 
		,right(sap.processing_branch_cd,3) as branch_cd 
		,sap.doc_type as sap_doc_type 
		,sap.reference_header
		,(date_trunc('month',sap.posting_date_dt) + interval '1 month -1 day')::date as posting_date_dt
		,(date_trunc('month',sap.doc_dt) + interval '1 month -1 day')::date as  doc_dt,sap.doc_no
		,sap.account_no as accode ,sap.dc_flg
		,sap.posting_sap_amt as posting_amt 
		,sap.policy_no , coalesce(sap.plan_cd,pol.plan_cd) as plan_cd
		,sap.posting_sap_amt as sum_natrn_amt 
		,sap.posting_sap_amt as posting_sap_amt 
		,sap.posting_sap_amt as posting_proxy_amt  
		,sap.selling_partner,sap.distribution_mode,sap.product_group,sap.product_sub_group,sap.rider_group,sap.term_cd,right(sap.cost_center,8) cost_center
		,sap_dim_txt as nac_dim_txt 
		,'gl_proxy' as source_filename 		
		, 'Manual'::varchar(100) as  group_nm 
		, 'Accruedac_current'::varchar(100) as subgroup_nm 
		, 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, sap.description_txt  
		, coa.event_type , coa.comm_gmm_flg , coa.comm_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		from stag_a.stga_ifrs_nac_txn_step01  sap
		inner join stag_s.ifrs_common_coa coa 
		on ( sap.account_no = coa.accode 
		and coa.event_type  = 'Commission')
		left outer join dds.tl_acc_policy_chg pol  
		on ( sap.policy_no  = pol.policy_no
		and sap.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join  stag_s.stg_tb_core_planspec pp 
		on ( pol.plan_cd = pp.plan_cd) 
		inner join stag_s.ifrs_common_variable_nm vn
		on ( vn.event_type = 'Commission'
		and vn.variable_cd = 'V3'
		and coa.comm_gmm_flg = vn.comm_gmm_flg
		and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )     
		where sap.posting_date_dt between v_control_start_dt and v_control_end_dt
		and coa.event_type  = 'Commission' 
		and coa.comm_gmm_flg = '1'
		and sap.doc_type not in ( 'SI','SB','SA')  -- SA depend V3 
		and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_commission c 
							where sap.ref1_header = c.ref_1 
							and sap.doc_dt = c.doc_dt
							and sap.doc_type = c.sap_doc_type 
							and sap.account_no = c.accode 
							and c.variable_cd in ( 'V2','V3') 
							) ;
        /*and  not exists ( select partner_cd from stag_f.ifrs_common_partner_dim pt 
							where selling_partner_desc = 'Agency' 
							and pt.selling_partner = sap.selling_partner);	 
		*/
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP18 VX Manual PROXY : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP18 % : % row(s) - VX - Manual PROXY ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	
 	*/
	--------------------
 
 	BEGIN	
		-- Reconcile 
		drop table if exists stag_a.stga_ifrs_nac_txn_missing_step05_commission;
		create table stag_a.stga_ifrs_nac_txn_missing_step05_commission tablespace tbs_stag_a as 
		select step4.* 
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_commission step5
		on ( step4.ref_1 = step5.ref_1
		and step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt
		and step4.doc_type = step5.doc_type
		and step4.doc_no = step5.doc_no ) 
		where step5.ref_1 is null
		-- 24Jan2023 : K.Ball ยกเลิก V3
		--and step4.sap_doc_type not in ( 'SA')  -- SA depend V3
		and step4.doc_dt between v_control_start_dt and v_control_end_dt 
		and step4.accode in ( select accode from stag_s.ifrs_common_coa where event_type = 'Commission');
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 		raise notice 'STEP19 % : % row(s) - Reconcile ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
 	/*
 		-- condition in V3
 		delete from stag_a.stga_ifrs_nac_txn_missing_step05_commission c
 		where sap_doc_type = 'SI' 
 		and dc_flg <> 'D';
 		 and exists ( select 1 
 			from stag_s.ifrs_common_partner_dim pt 
 			where selling_partner_desc = 'Agency' and pt.selling_partner = c.selling_partner);*/
 		raise notice 'STEP19.1 % : % row(s) - Remove condition in V3 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_insert_txn05_commission' 				
				,'ERROR STEP18 Reconcile : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;	 

 	--raise notice 'STEP19.2 % : % row(s) - Reconcile ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  

 	BEGIN
		------------- DUMMY    
		insert into stag_a.stga_ifrs_nac_txn_step05_commission  
		select ref_1,nac_rk,natrn_x_dim_rk,branch_cd,sap_doc_type,reference_header,post_dt
		,doc_dt,doc_type,doc_no,accode,dc_flg,actype ,posting_amt
		,system_type,transaction_type,premium_type,policy_no,plan_cd,rider_cd
		,pay_dt,pay_by_channel,sum_natrn_amt,posting_sap_amt,posting_proxy_amt
		,sales_id,sales_struct_n,selling_partner,distribution_mode,product_group
		,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
		,source_filename,group_nm,subgroup_nm,subgroup_desc
		,variable_cd,variable_nm,nadet_detail
		,org_branch_cd,org_submit_no,rp_no,submit_no
		,section_order_no,is_section_order_no
		,for_branch_cd,event_type,comm_gmm_flg,comm_gmm_desc,policy_type
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
			,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
			,natrn.filename as source_filename  
			, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'Commission-Dummy NonAgent' ::varchar(100) as subgroup_desc  
			, case when natrn.nadet_policyno is not null then coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') else coa.dummy_variable_nm END as variable_cd
			, case when natrn.nadet_policyno is not null then coalesce (vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') else coa.variable_for_policy_missing end as variable_nm  
			, natrn.detail as nadet_detail 
			, natrn.org_branch_cd, null::varchar as org_submit_no
			, null::varchar as rp_no , null::varchar as submit_no
			, null::varchar as section_order_no
			, 0 as is_section_order_no
			,natrn.for_branch_cd
			,natrn.event_type 
			,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
									order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd) as _rk 
			from  stag_a.stga_ifrs_nac_txn_missing_step05_commission natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )  
			left join  stag_s.stg_tb_core_planspec pp 
			on ( natrn.plan_cd = pp.plan_cd) 
			left outer join dds.tl_acc_policy_chg pol  
			on ( natrn.nadet_policyno  = pol.policy_no
			and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm 
			and pol.policy_type not in ( 'M','G')) -- Oil 25jan2023 : policy_type not in ( 'M','G')
			left outer join stag_s.ifrs_common_coa coa 
			on ( natrn.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Commission' 
			and vn.variable_cd = 'V2' 
			and natrn.event_type  = vn.event_type
			and natrn.comm_gmm_flg  = vn.comm_gmm_flg  ) 
			where natrn.post_dt  between v_control_start_dt and v_control_end_dt 
			and sales_id is null  
			 
			union all  
			
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
			,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
			,natrn.filename as source_filename  
			, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'Commission-Dummy  Agent' ::varchar(100) as subgroup_desc  
			, case when natrn.nadet_policyno is not null then coalesce (vn.variable_cd,coa.dummy_variable_nm,'DUM_NT') else coa.dummy_variable_nm END as variable_cd
			, case when natrn.nadet_policyno is not null then coalesce (vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') else coa.variable_for_policy_missing end as variable_nm  
			, natrn.detail as nadet_detail 
			, natrn.org_branch_cd, null::varchar as org_submit_no
			, null::varchar as rp_no , null::varchar as submit_no
			, null::varchar as section_order_no
			, 0 as is_section_order_no
			,natrn.for_branch_cd
			,natrn.event_type 
			,natrn.comm_gmm_flg ,natrn.comm_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
			, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
									order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd) as _rk 
			from  stag_a.stga_ifrs_nac_txn_missing_step05_commission natrn 
			left join  stag_s.ifrs_common_accode acc 
			on ( natrn.accode = acc.account_cd  )  
			left join  stag_s.stg_tb_core_planspec pp 
			on ( natrn.plan_cd = pp.plan_cd) 
			left outer join dds.tl_acc_policy_chg pol  
			on ( natrn.nadet_policyno  = pol.policy_no
			and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm 
			and pol.policy_type not in ( 'M','G')) -- Oil 25jan2023 : policy_type not in ( 'M','G')
			left outer join stag_s.ifrs_common_coa coa 
			on ( natrn.accode = coa.accode  )  
			left join stag_s.ifrs_common_variable_nm vn 
			on ( vn.event_type = 'Commission'
			and vn.variable_cd = 'V10' 
			and natrn.event_type = vn.event_type
			and natrn.comm_gmm_flg  = vn.comm_gmm_flg
			and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )  
			where natrn.post_dt  between v_control_start_dt and v_control_end_dt 
			and natrn.sales_id is not null 
			
		)  as aa 
		where _rk =1 ; 
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_Commission' 				
				,'ERROR STEP19 VX Dummy: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP19 % : % row(s) - VX - Dummy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   
 
	--=============== Exclude GROUPEB ====================--
 	begin   
	 
		delete from stag_a.stga_ifrs_nac_txn_step05_commission 
		where plan_cd   in (  'M907','GEB1','GEB2' ) ; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP20.1 % : % row(s) - Exclude GROUPEB ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
	
		-- 20Jul2023 Narumol W.:Commission Log 364 Exclude is_cutoff = 1 
		delete from stag_a.stga_ifrs_nac_txn_step05_commission  p
		where event_type  = 'Commission' 
		and exists ( select 1 from  dds.ifrs_suspense_newcase_chg nc
					where doc_dt  between v_control_start_dt and v_control_end_dt 
					and p.doc_dt = nc.doc_dt 
					and p.doc_type = nc.doc_type 
					and p.doc_no = nc.doc_no 
					and p.accode = nc.c_accode 
					and nc.is_cut_off = '1'  );
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP20.2 % : % row(s) - Exclude suspense is_cutoff = 1 ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		-- 02Oct2023 Narumol W. : Patch policy_no and plan_cd
		update stag_a.stga_ifrs_nac_txn_step05_commission p
		set  policy_no = pol.policy_no 
		,plan_cd = pol.plan_cd
		from dds.tl_acc_policy_chg pol 
		where ( p.policy_no = pol.plan_cd || pol.policy_no 
		and p.doc_dt between pol.valid_fr_dttm  and pol.valid_to_dttm ) 
		and length(p.policy_no) = 12 
		and p.sap_doc_type in ('SI','SB');
		raise notice 'STEP20.3 % : % row(s) - Patch policy_no and plan_cd ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_commission' 				
				,'ERROR STEP20 Exclude GROUPEB: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  

	-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
	begin
		
		if date_part('month',v_control_start_dt) = 1  then  
		
			insert into stag_a.stga_ifrs_nac_txn_step05_commission 
			select ref_1 ,nac_rk ,natrn_x_dim_rk ,branch_cd ,sap_doc_type
			,reference_header 
			,v_control_end_dt as post_dt
			,doc_dt ,doc_type ,doc_no
			,accode ,dc_flg ,actype
			,posting_amt * -1 as posting_amt
			,system_type ,transaction_type ,premium_type ,policy_no
			,plan_cd ,rider_cd ,pay_dt ,pay_by_channel ,sum_natrn_amt
			,posting_sap_amt ,posting_proxy_amt ,sales_id ,sales_struct_n
			,selling_partner ,distribution_mode ,product_group ,product_sub_group
			,rider_group ,product_term ,cost_center ,nac_dim_txt ,source_filename ,group_nm
			,'Accrued_bop' as subgroup_nm
			,subgroup_desc ,variable_cd ,variable_nm ,nadet_detail ,org_branch_cd
			,org_submit_no ,submit_no ,rp_no ,section_order_no ,is_section_order_no
			,for_branch_cd ,event_type ,comm_gmm_flg ,comm_gmm_desc ,policy_type
			,effective_dt ,issued_dt ,ifrs17_channelcode ,ifrs17_partnercode
			,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup ,is_duplicate_variable
			,log_dttm
			from 
			dds.ifrs_variable_commission_ytd a
			where a.control_dt =  v_control_end_dt - interval '1 month'
			and variable_nm in ('ACTUAL_ACQCOM_ADV_PD');
			
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		end if;
		
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_Commission' 				
				,'ERROR STEP19 VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP19 % : % row(s) - VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  
	--=============== Insert into YTD ==================--
    select  dds.fn_ifrs_variable_commission(p_xtr_start_dt) into out_err_cd  ;
 
  	-- Complete
	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.ifrs_variable_txn05_commission','COMPLETE: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

	raise notice 'STEP20 % : % row(s) - Complete',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   

   	return i;
END

$function$
;
