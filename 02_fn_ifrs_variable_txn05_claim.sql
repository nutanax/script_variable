CREATE OR REPLACE FUNCTION dds.fn_ifrs_variable_txn05_claim(p_xtr_start_dt date)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
-- =============================================  
-- Author:  Narumol W.
-- Create date: 2021-05-25
-- Description: For insert event data 
-- Oil 2022Jul15 : ยกเลิก V2,V3
-- Oil 2022Aug26 : Add COALESCE(gclm.policy_no,'X') log 214
-- 19Aug2022 Narumol W. ยกเลิก Filter branch_cd = '000 เนื่องจาก เอา V2 มารวมด้วย
-- 08Sep2022 Narumol W.: Claim Log 206 Remove condition unpaid approve claim 
-- Oil 08Sep2022 : K.Ball cf เพิ่ม Source fax claim บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน
-- Oil 08Sep2022 : ย้ายไป V1 [SEQ NO : 8]
-- Oil 08Sep2022: Add coalesce Log.215
-- Oil 28Sep2022 : Add claim_gmm_flg <> '5' Log.221
-- Oil 07Oct2022 : เปลี่ยนชื่อเทเบิล dds.ifrs_adth_fax_claim_chg >> ifrs_adth_payment_api_chg
-- Oil 07Nov2022 : Claim Log 232 Case Policy doc_no 5 (Trach change 47) 
-- Oil 25Nov2022 : เพิ่ม policy_no , plan_cd 
-- 18Jan2023 Narumol W.: Log.227 Use natrn.ref_1 instead of accru.ref1
-- Oil 25jan2023 : policy_type not in ( 'M','G')
-- Oil 25jan2023 : add plan_cd 
-- 10Feb2023 Narumol W. : update icgid for PAA
-- 15Mar2023 Narumol W.: Claim Log 345 Add new source 
-- >> เดิมบันทึกบัญชีที่การเงิน ข้อมูลอยู่ที่แฟ้ม glife ส่วนที่หลังจากขึ้นระบบและจะลงที่แฟ้ม payment นั้น ควรเช็คจากวันที่ลูกค้าได้รับเงิน หรือ payment date >= 2023-01-06
--  select dds.fn_ifrs_variable_txn05_claim('2022-02-20'::date)
--  select dds.fn_ifrs_variable_txn05_claim('2022-01-20'::date)
--  select * from dds.tb_fn_log order by log_dttm desc 
-- Oil 20230516 : Add V11 stag_f.tb_sap_trial_balance>> ปรับใช้  dds.tb_sap_trial_balance
-- 03July2023 Narumol W. : [ITRQ#66062868] เปลี่ยนแปลง sign ของ CLAIM_YTD Var.6 และ BENEFIT Var.3
-- 14Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
-- 02Oct2023 Narumol W. : Patch policy_no and plan_cd
-- 25Oct2023 Narumol W. : Add step join with policy and plan_cd after patch policy & plan_Cd 
-- 19Oct2023 Narumol W.: [ITRQ#66104655] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
-- 13Nov2023 Narumol W. : Claim Log 401 Trim white space
-- 23Feb2024 Narumol W.: [ITRQ#67020753] เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link
-- 06Aug2024 Narumol W.: [ITRQ#67073483] เพิ่ม Source accrued-ac ใน Variable Unpaid Approve Claim
-- 17Oct2024 Narumol W.: [ITRQ#67094481] เพิ่ม source ของบัญชีสัญญาพิเศษเพิ่มเติมค้างจ่าย-รักษาพยาบาล(รพ.)
-- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
-- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
-- 14Jul2024 Nuttadet O. : [ITRQ#68062201] เพิ่ม call stored dds.fn_ifrs_variable_txn05_imo_claim
-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
-- =============================================  
declare 
	i int default  0;
	v_control_start_dt date;
	v_control_end_dt date;
	out_err_cd int default 0;
	out_err_msg varchar(200);
	v_affected_rows int;
	v_ra_rate numeric(10,2);
	
begin  
	-- select dds.fn_ifrs_variable_txn05_claim('2024-01-20'::date)
	-- select * from dds.tb_fn_log order by log_dttm desc  
	raise notice 'START % : % row(s) - ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.ifrs_variable_txn05_claim','START: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	

	if p_xtr_start_dt is not null then 
		v_control_start_dt := date_trunc('month', p_xtr_start_dt - interval '1 month');
		v_control_end_dt := v_control_start_dt + interval '1 month -1 day' ;
	end if;
	raise notice 'v_control_start_dt :: % ', v_control_start_dt::VARCHAR(10);
	raise notice 'v_control_end_dt :: % ', v_control_end_dt::VARCHAR(10);

	-- Prepare missing plancode select * from stag_s.stg_tb_core_planspec
	insert into stag_s.stg_tb_core_planspec ( plan_cd , ifrs17_var_current , ifrs17_var_future , index_flg )
	select plan_cd , ifrs_var_current,ifrs17_var_future,plan_index
	from (   
	 select 'GEB1' as plan_cd ,'Y' as ifrs_var_current ,null as ifrs17_var_future,'TERM' as plan_index union all 
	 select 'GEB2' as plan_cd ,'Y' as ifrs_var_current ,null as ifrs17_var_future,'TERM' as plan_index  union all 
	 select 'PL34' as plan_cd ,'Y' as ifrs_var_current ,'Y' as ifrs17_var_future ,'TERM' as plan_index   union all 
	 select 'M907' as plan_cd ,'Y' as ifrs_var_current ,'Y' as ifrs17_var_future ,'TERM' as plan_index   
	 ) pl   
	 where plan_cd not in ( select plan_cd from stag_s.stg_tb_core_planspec  );
	
	-- Prepare accrual 
	begin
		select dds.fn_dynamic_vw_ifrs_accrual_chg (v_control_start_dt , v_control_end_dt) into out_err_cd;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			return out_err_cd;
	end;
	
	begin
		select dds.fn_dynamic_vw_ifrs_accrual_eoy_chg(v_control_start_dt) into out_err_cd;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			return out_err_cd;
	end;

	begin
		select  dds.fn_ifrs_variable_txn01_04_eoy(p_xtr_start_dt) into out_err_cd;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			return out_err_cd;
	end;

	-- v_ra_rate := 20; 
	select max(ra_rate) 
	into v_ra_rate
	from dds.ifrs_common_ra_rate ;  
 	raise notice 'STEP00 % : v_ra_rate = % ',clock_timestamp()::varchar(19),v_ra_rate::varchar;

	begin
		drop table if exists stag_a.stga_sum_ifrs_claim_trans;
		create table stag_a.stga_sum_ifrs_claim_trans tablespace tbs_stag_a as 
		--truncate table stag_a.stga_sum_ifrs_claim_trans;
		--insert into stag_a.stga_sum_ifrs_claim_trans 
		select * 
		from (   
		select section_order_no , policy_no 
		, receive_dt ,accident_dt,claimok_dt,pay_dt,groupeb_certno as groupeb_cert_no
		, ROW_NUMBER() OVER(
   	 		PARTITION BY section_order_no , policy_no 
    		ORDER by section_order_no, policy_no , pay_dt desc ) as _rk 
		from  stag_a.stga_ifrs_claim_trans  
		where trim(section_order_no) <> ''  
		and pay_dt  between v_control_start_dt and v_control_end_dt ) as aa 
		where _rk = 1 ;
	
		CREATE INDEX stga_stga_sum_ifrs_claim_trans_idx_1 ON stag_a.stga_sum_ifrs_claim_trans USING btree (section_order_no,policy_no);
 
 		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			return out_err_cd;
	end;

 	BEGIN
		truncate table stag_a.stga_ifrs_nac_txn_step05_claim ;
	  	--drop table if exists stag_a.stga_ifrs_nac_txn_step05_claim ;
	  	--create table stag_a.stga_ifrs_nac_txn_step05_claim tablespace tbs_stag_a as 

		-- 14Jul2024 Nuttadet O. : [ITRQ#68062201] เพิ่ม call stored dds.fn_ifrs_variable_txn05_imo_claim
	 	select dds.fn_ifrs_variable_txn05_imo_claim(p_xtr_start_dt) into out_err_cd  ;																				 
		-- 1. บันทึกจ่ายสินไหม
		insert into stag_a.stga_ifrs_nac_txn_step05_claim   
		select  natrn.ref_1 as ref_1 , adth.adth_rk ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
		--, adth.adth_pay_rk as adth_pay_rk , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header::varchar(100) ,natrn.post_dt
		, adth.receive_dt as claim_dt
		, adth.accident_dt as accident_dt
		, adth.claim_ok_dt 
		,adth.ref_doc_dt doc_dt,natrn.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.posting_claim_amt as posting_claim_amt, adth.claim_amt  as claim_amt 
		,adth.posting_claim_amt as posting_claim_pay_amt, adth.claim_amt  as claim_pay_amt  
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd  
		,null::varchar as pay_status_cd ,null::date as  pay_dt ,null::varchar as pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n    as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as nac_dim_txt  
		,adth.source_filename::varchar(100) , 'natrn-claim'::varchar(100) as  group_nm  , 'claim'::varchar(100)  as subgroup_nm ,  'บันทึกจ่ายสินไหม'::varchar(100)  as subgroup_desc 
		 , vn.variable_cd
		, vn.variable_name as variable_nm  
		, natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar(20) as submit_no
		, adth.section_order_no
		, 0::int as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm  
		from  stag_a.stga_ifrs_nac_txn_step04  natrn
		 inner join  dds.oic_adth_chg  adth --dds.ifrs_adth_pay_chg adth
		 on (   adth.branch_cd = natrn.branch_cd  
				and adth.ref_doc_dt = natrn.doc_dt  
				 and adth.ref_doc_no = natrn.doc_no 
				 and adth.accode = natrn.accode 
				 and adth.dc_flg = natrn.dc_flg   
				and adth.adth_dim_txt = natrn.natrn_dim_txt )
				-- and coalesce(adth.pay_status_cd,'') not in ( 'N','C','R')) -- Log.168,129
		 left join stag_s.ifrs_common_accode  acc 
		 on ( natrn.accode = acc.account_cd  )
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg = natrn.claim_gmm_flg 
		 )
		left outer join dds.tl_acc_policy_chg pol  
		 on (  adth.policy_no = pol.policy_no
		 and  adth.plan_cd = pol.plan_cd
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		where  natrn.event_type = 'Claim' 
		 and adth.pay_type_cd <> '2' 
		 and adth.org_branch_cd not in ( 'GCL','CCL')  
		 -- 19Aug2022 Narumol W. ยกเลิก Filter branch_cd = '000 เนื่องจาก เอา V2 มารวมด้วย
		 --and adth.branch_cd = '000' --Log.83 branch -> V2 <-- ยกเลิก Filter เนื่องจาก เอา V2 มารวมด้วย
		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;  
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP01 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END; 
 	raise notice 'STEP01 % : % row(s) - V1 บันทึกจ่ายสินไหม',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	begin -- select * from stag_f.ifrs_gclm_bnf_chg 
		-- GCLM 
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
 		select  natrn.ref_1 as ref_1 , gclm.gclm_rk ,null::bigint gclm_bnf_rk , natrn.natrn_x_dim_rk  
        ,natrn.branch_cd  
        ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        ,claim.receive_dt as claim_dt 
        ,claim.accident_dt as accident_dt 
        ,claim.claimok_dt  as claim_ok_dt  
        ,gclm.doc_dt,gclm.doc_type,gclm.doc_no,gclm.accode,gclm.dc_flg,null::varchar as actype  
        ,gclm.posting_claim_amt as posting_claim_amt ,gclm.total_claim_amt claim_amt  
        ,gclm.posting_claim_amt ,gclm.total_claim_amt 
        ,null::varchar system_type,null::varchar transaction_type,null::varchar as premium_type
        --,coalesce(claim.groupeb_cert_no,gclm.policy_no) as policy_no
        ,gclm.policy_no
        ,gclm.plan_cd, gclm.rider_cd , gclm.pay_flg pay_status_cd ,gclm.pay_dt ,null::varchar as pay_by_channel
        ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        ,gclm.sales_id,gclm.sales_struct_n,gclm.selling_partner,gclm.distribution_mode,gclm.product_group,gclm.product_sub_group,gclm.rider_group,gclm.product_term,gclm.cost_center
        ,gclm.gclm_dim_txt  
        ,'gclmbnf' as source_filename , 'natrn-gclm' as  group_nm  , 'claim-AP รพ' as subgroup_nm ,  'บันทึกค้างจ่ายสินไหม gclm - GROUP EB' as subgroup_desc  
         , vn.variable_cd
        , vn.variable_name as variable_nm    
        ,natrn.detail as nadet_detail 
        , natrn.org_branch_cd,null::varchar as org_submit_no
        , null::varchar as submit_no
        , gclm.claim_no section_order_no
        , 0 as is_section_order_no
        ,natrn.for_branch_cd
        ,coa.event_type  
        ,coa.claim_gmm_flg,coa.claim_gmm_desc  
        ,gclm.business_type as policy_type ,null::date effective_dt ,null::date issued_dt 
        ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        ,pp.ifrs17_portid ,pp.ifrs17_portgroup
        ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm  
        from  stag_a.stga_ifrs_nac_txn_step04  natrn 
        inner join  dds.ifrs_gclm_chg gclm -- dds.ifrs_gclm_bnf_chg gclm   
         on (   gclm.for_branch_cd = natrn.for_branch_cd  
                and gclm.doc_dt = natrn.doc_dt  
                and gclm.doc_type = natrn.doc_type 
                 and gclm.doc_no = natrn.doc_no 
                and gclm.accode = natrn.accode 
                 and gclm.dc_flg = natrn.dc_flg 
                and gclm.gclm_dim_txt = natrn.natrn_dim_txt ) 
         left outer join stag_a.stga_sum_ifrs_claim_trans claim 
         on (gclm.claim_no = claim.section_order_no
		 and gclm.policy_no = claim.policy_no ) 
         left join stag_s.ifrs_common_accode  acc 
         on ( gclm.accode = acc.account_cd  ) 
         left outer join stag_s.ifrs_common_coa coa  
         on ( gclm.accode = coa.accode )
         left join  stag_s.stg_tb_core_planspec pp 
         on ( gclm.plan_cd = pp.plan_cd)
         inner join stag_s.ifrs_common_variable_nm vn  
         on ( vn.event_type = 'Claim'
         and vn.variable_cd = 'V1'
         and vn.claim_gmm_flg= natrn.claim_gmm_flg 
         ) 
         where  coa.event_type  = 'Claim' 
         -- and gclm.pay_flg = '1'  -- Log.129
         and COALESCE(gclm.policy_no,'X')  <> '0032000874101'--log.208, 2022Aug26 oil : COALESCE(gclm.policy_no,'X') log 214
         and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP02 V1-GCLM : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP02 % : % row(s) - V1 บันทึกจ่ายสินไหม gclm',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	-- ================================== V1 โอนเงินสินไหมผ่านธ. กรุงศรีฯ  =================================================================--

  	begin -- select * from   dds.ifrs_glife_actrf_chg
	  	-- 15Mar2023 Narumol W.: Claim Log 345 Add new source 
	  	if v_control_start_dt <= '2023-01-01'::date then 
			-- glife_actrf 
			insert into stag_a.stga_ifrs_nac_txn_step05_claim  
	 		select  natrn.ref_1 as ref_1 , actrf.glife_actrf_rk  ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
			,natrn.branch_cd  
			,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
			,claim.receive_dt as claim_dt 
			,claim.accident_dt as accident_dt 
			,claim.claimok_dt  as claim_ok_dt 
			,actrf.doc_dt,actrf.doc_type,actrf.doc_no,actrf.accode,actrf.dc_flg,null::varchar as actype  
			,actrf.posting_claim_amt as posting_claim_amt ,actrf.total_claim_amt claim_amt  
			,actrf.posting_claim_amt ,actrf.total_claim_amt
			,null::varchar system_type,null::varchar transaction_type,null::varchar as premium_type
			,coalesce( claim.groupeb_cert_no , claim.policy_no) as policy_no ,actrf.plan_cd, actrf.rider_cd , null::varchar pay_status_cd ,actrf.pay_dt ,null::varchar as pay_by_channel
			,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
			,actrf.sales_id,actrf.sales_struct_n,actrf.selling_partner,actrf.distribution_mode,actrf.product_group,actrf.product_sub_group,actrf.rider_group,actrf.product_term,actrf.cost_center
			,actrf.actrf_dim_txt  
			,'glife_actrf' as source_filename , 'natrn-glife_actrf' as  group_nm  , 'โอนเงินสินไหมผ่านธ. กรุงศรีฯ' as subgroup_nm ,  'โอนเงินสินไหมผ่านธ. กรุงศรีฯ' as subgroup_desc   
	 		, vn.variable_cd
			, vn.variable_name as variable_nm   
			,natrn.detail as nadet_detail 
			, natrn.org_branch_cd,null::varchar as org_submit_no
			, null::varchar as submit_no
			, actrf.claim_no section_order_no
			, 0 as is_section_order_no
			,natrn.for_branch_cd
			,coa.event_type 
			,coa.claim_gmm_flg,coa.claim_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup
			,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
			from  stag_a.stga_ifrs_nac_txn_step04  natrn 
			 inner join dds.ifrs_glife_actrf_chg  actrf 
			 on (   actrf.branch_cd = natrn.for_branch_cd  
				and actrf.doc_dt = natrn.doc_dt  
				and actrf.doc_type = natrn.doc_type 
			 	and actrf.doc_no = natrn.doc_no 
				and actrf.accode = natrn.accode 
		 		and actrf.dc_flg = natrn.dc_flg 
				and actrf.actrf_dim_txt = natrn.natrn_dim_txt  	)
			 left outer join  stag_a.stga_sum_ifrs_claim_trans claim   
			 on ( actrf.claim_no = claim.section_order_no)
			 left join stag_s.ifrs_common_accode  acc 
			 on ( actrf.accode = acc.account_cd  ) 
			 left outer join stag_s.ifrs_common_coa coa  
			 on ( actrf.accode = coa.accode )
			 left join  stag_s.stg_tb_core_planspec pp 
			 on ( actrf.plan_cd = pp.plan_cd)
			 inner  join stag_s.ifrs_common_variable_nm vn
			 on ( vn.event_type = 'Claim'
			 and vn.variable_cd = 'V1'
			 and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
			 ) 
			left outer join dds.tl_acc_policy_chg pol  
			 on (  claim.policy_no = pol.policy_no
			and  actrf.plan_cd = pol.plan_cd 
			 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			 where  coa.event_type  = 'Claim'  
			 -- 15Mar2023 Narumol W.: Claim Log 345 Add new source   
			 and actrf.pay_dt <  '2023-01-06'
			 and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
		end if;
			
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP03 V1-glife_actrf : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP03 % : % row(s) - V1 บันทึกจ่ายสินไหม gclm',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- 15Mar2023 Narumol W.: Claim Log 345 Add new source  
  	begin  
	  	-- 15Mar2023 Narumol W.: Claim Log 345 Add new source  
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
	 	select  natrn.ref_1 as ref_1 , paytrans.payment_transpos_rk  ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd  
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,claim.receive_dt as claim_dt 
		,claim.accident_dt as accident_dt 
		,claim.claimok_dt  as claim_ok_dt 
		,paytrans.doc_dt,paytrans.doc_type,paytrans.doc_no,paytrans.accode,paytrans.dc_flg,null::varchar as actype  
		,paytrans.posting_refund_amt  as posting_claim_amt ,paytrans.posting_refund_amt claim_amt  
		,paytrans.posting_refund_amt ,paytrans.posting_refund_amt
		,paytrans.system_type,paytrans.transaction_type,paytrans.premium_type
		,paytrans.policy_no  
		,paytrans.plan_cd, paytrans.rider_cd , null::varchar pay_status_cd ,paytrans.refund_dt  ,null::varchar as pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,paytrans.sales_id,paytrans.sales_struct_n,paytrans.selling_partner,paytrans.distribution_mode,paytrans.product_group,paytrans.product_sub_group,paytrans.rider_group,paytrans.product_term,paytrans.cost_center
		,paytrans.transpos_dim_txt   
        , 'paytrans'::varchar as source_filename  , 'natrn-paytrans' as  group_nm  , 'epayment' as subgroup_nm ,'โอนเงินสินไหมผ่านธ. กรุงศรีฯ'  as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm   
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, paytrans.refund_key  section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,coa.event_type 
		,coa.claim_gmm_flg,coa.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04  natrn 
		 inner join dds.ifrs_payment_transpos_chg  paytrans  
                on ( natrn.branch_cd =  paytrans.branch_cd 
                and natrn.doc_dt = paytrans.doc_dt 
                and natrn.doc_type =  paytrans.doc_type 
                and natrn.doc_no = paytrans.doc_no 
                and  natrn.dc_flg = paytrans.dc_flg
                and natrn.accode = paytrans.accode 
                and natrn.natrn_dim_txt = paytrans.transpos_dim_txt  ) 
		 left outer join  stag_a.stga_sum_ifrs_claim_trans claim   
		 on (  paytrans.refund_key  = claim.section_order_no  
		 and paytrans.policy_no = claim.policy_no
		 and claim._rk = 1)
		 left join stag_s.ifrs_common_accode  acc 
		 on ( paytrans.accode = acc.account_cd  ) 
		 left outer join stag_s.ifrs_common_coa coa  
		 on ( paytrans.accode = coa.accode )
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( paytrans.plan_cd = pp.plan_cd)
		 inner  join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg  ) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  claim.policy_no = pol.policy_no
		 and  paytrans.plan_cd = pol.plan_cd 
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		where coa.event_type  = 'Claim'   
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ; 
			
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP03.1 V1-payment trans : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP03.1 % : % row(s) - V1-payment trans',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- 15Mar2023 Narumol W.: Claim Log 345 Add new source  
  	begin  
	  	-- 15Mar2023 Narumol W.: Claim Log 345 Add new source  
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
	 	select  natrn.ref_1 as ref_1 , reject.payment_reject_rk  ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd  
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,claim.receive_dt as claim_dt 
		,claim.accident_dt as accident_dt 
		,claim.claimok_dt  as claim_ok_dt 
		,reject.reject_doc_dt as doc_dt,reject.reject_doc_type as doc_type,reject.reject_doc_no as doc_no
		,reject.accode,reject.dc_flg,null::varchar as actype  
		,reject.reject_posting_amt  as posting_claim_amt ,reject.reject_amt claim_amt  
		,reject.reject_posting_amt ,reject.reject_amt
		,reject.system_type,reject.transaction_type,null::varchar premium_type
		,reject.policy_no  
		,reject.plan_cd, reject.rider_cd , null::varchar pay_status_cd ,reject.refund_dt  ,null::varchar as pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n,reject.selling_partner,reject.distribution_mode,reject.product_group,reject.product_sub_group,reject.rider_group,reject.product_term,reject.cost_center
		,reject.natrn_dim_txt    
        , 'reject_paytrans'::varchar as source_filename  , 'natrn-reject' as  group_nm  , 'Reject epayment' as subgroup_nm ,'Reject โอนเงินสินไหมผ่านธ. กรุงศรีฯ'  as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm   
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, reject.refund_key  section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,coa.event_type 
		,coa.claim_gmm_flg,coa.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04  natrn 
		inner join  dds.ifrs_payment_reject_chg reject  
       	on ( natrn.branch_cd =  reject.branch_cd 
      	and natrn.doc_dt = reject.reject_doc_dt 
     	and natrn.doc_type =  reject.reject_doc_type 
      	and natrn.doc_no = reject.reject_doc_no 
       	and natrn.dc_flg = reject.dc_flg
      	and natrn.accode = reject.accode 
   		and natrn.natrn_dim_txt = reject.natrn_dim_txt  ) 
		 left outer join  stag_a.stga_sum_ifrs_claim_trans claim   
		 on (  reject.refund_key  = claim.section_order_no  
		 and reject.policy_no = claim.policy_no
		 and claim._rk = 1)
		 left join stag_s.ifrs_common_accode  acc 
		 on ( reject.accode = acc.account_cd  ) 
		 left outer join stag_s.ifrs_common_coa coa  
		 on ( reject.accode = coa.accode )
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( reject.plan_cd = pp.plan_cd)
		 inner  join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg  ) 
		left outer join dds.tl_acc_policy_chg pol  
		 on (  claim.policy_no = pol.policy_no
		and  reject.plan_cd = pol.plan_cd 
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 where coa.event_type  = 'Claim'  
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ; 
			
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP03.2 V1-payment reject : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP03.2 % : % row(s) - V1-payment reject',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	 -- 06Jun2025 Nuttadet O. : [ITRQ.68052055] เพิ่ม src dds.ifrs_payment_api_reverse_chg
  	begin  
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  natrn.ref_1 as ref_1 , reject.payment_api_reverse_rk  ,null::bigint as adth_pay_rk 
		, natrn.natrn_x_dim_rk  , natrn.branch_cd  
        , natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
        , claim.receive_dt as claim_dt 
        , claim.accident_dt as accident_dt 
        , claim.claimok_dt  as claim_ok_dt 
        , reject.doc_dt as doc_dt,reject.doc_type as doc_type,reject.doc_no as doc_no
        , reject.accode,reject.dc_flg,null::varchar as actype  
        , reject.posting_payment_api_reverse_amt  as posting_claim_amt ,reject.payment_api_reverse_amt claim_amt  
        , reject.posting_payment_api_reverse_amt ,reject.payment_api_reverse_amt
        , reject.system_type,reject.transaction_type,null::varchar premium_type
        , reject.policy_no  
        , reject.plan_cd, reject.rider_cd , null::varchar pay_status_cd ,reject.refund_dt  
        , null::varchar as pay_by_channel
        , natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
        , natrn.sales_id,natrn.sales_struct_n,reject.selling_partner ,reject.distribution_mode 
        , reject.product_group ,reject.product_sub_group ,reject.rider_group ,reject.product_term 
        , reject.cost_center 
        , reject.reverse_dim_txt    
        , 'paymentapi_reverse'::varchar as source_filename  , 'natrn-paymentapi_reverse' as  group_nm  
        , 'paymentapi_reverse' as subgroup_nm ,'PAYMENT API Reverse กลับรายการโอนเงินไม่สำเร็จ'  as subgroup_desc 
        , vn.variable_cd
        , vn.variable_name as variable_nm   
        , natrn.detail as nadet_detail 
        , natrn.org_branch_cd,null::varchar as org_submit_no
        , null::varchar as submit_no
        , reject.refund_key  section_order_no
        , 0 as is_section_order_no
        , natrn.for_branch_cd
        , coa.event_type 
        , coa.claim_gmm_flg,coa.claim_gmm_desc 
        , pol.policy_type ,pol.effective_dt ,pol.issued_dt
        , pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
        , pp.ifrs17_portid ,pp.ifrs17_portgroup
        , vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
        from  stag_a.stga_ifrs_nac_txn_step04  natrn 
        inner join  dds.ifrs_payment_api_reverse_chg reject  
        on ( natrn.branch_cd  =  reject.branch_cd 
        and natrn.doc_dt = reject.doc_dt 
        and natrn.doc_type =  reject.doc_type 
        and natrn.doc_no = reject.doc_no 
        and natrn.dc_flg = reject.dc_flg
        and natrn.accode = reject.accode 
        and natrn.natrn_dim_txt = reject.reverse_dim_txt  ) 
         left outer join  stag_a.stga_sum_ifrs_claim_trans claim   
         on (  reject.refund_key  = claim.section_order_no  
         and reject.policy_no = claim.policy_no
         and claim._rk = 1)
         left join stag_s.ifrs_common_accode  acc 
         on ( reject.accode = acc.account_cd  ) 
         left outer join stag_s.ifrs_common_coa coa  
         on ( reject.accode = coa.accode )
         left join  stag_s.stg_tb_core_planspec pp 
         on ( reject.plan_cd = pp.plan_cd)
         inner  join stag_s.ifrs_common_variable_nm vn
         on ( vn.event_type = 'Claim'
         and vn.variable_cd = 'V1'
         and vn.claim_gmm_flg  = natrn.claim_gmm_flg  ) 
        left outer join dds.tl_acc_policy_chg pol  
         on (  claim.policy_no = pol.policy_no
        and  reject.plan_cd = pol.plan_cd 
         and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
         where coa.event_type  = 'Claim'  
		and natrn.post_dt between  v_control_start_dt and v_control_end_dt ; 
			
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP03.2 V1-payment api_reverse : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP03.2 % : % row(s) - V1-payment api_reverse',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
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

 	-- 13May :K.Ball CF Add source refund 
 	begin 
		
 		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  natrn.ref_1 as ref_1 , refund.refund_trans_rk  ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd  
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		, refund.doc_dt as claim_dt 
		, null::Date as accident_dt 
		, null::Date  as claim_ok_dt  
		, refund.doc_dt , refund.doc_type , natrn.doc_no, natrn.accode , natrn.dc_flg , null::varchar as actype
		, refund.posting_refund_amt as posting_amt , refund.refund_amt 
		, refund.posting_refund_amt as posting_amt , refund.refund_amt 
		, refund.system_type ,refund.transaction_type,refund.premium_type 
		, refund.policy_no as policy_no ,refund.plan_cd as plan_cd , refund.rider_cd as rider_cd ,null::varchar as pay_status_cd
		,refund.refund_dt  as pay_dt ,''::varchar as pay_by_channel  
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		, refund.sales_id,refund.sales_struct_n,refund.selling_partner,refund.distribution_mode,refund.product_group,refund.product_sub_group,refund.rider_group,refund.product_term,refund.cost_center
		, refund.refund_dim_txt as refund_dim_txt  
		, 'paytrans'::varchar as source_filename  , 'natrn-refund' as  group_nm  
		, 'Claim-Refund' as subgroup_nm ,'จ่ายคืนพักเบี้ยทั่วไป/พักเบี้ยเคสใหม่/คืนเบี้ยประกันรอจ่าย'  as subgroup_desc 
		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail 
		,refund.branch_cd as org_branch_cd , ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,refund.for_branch_cd
		,natrn.event_type 
		,natrn.claim_gmm_flg  ,natrn.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from  stag_a.stga_ifrs_nac_txn_step04 natrn 
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
		and refund.plan_cd = pol.plan_cd
		and refund.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	 	inner join stag_s.ifrs_common_variable_nm vn 
		  on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'  
		 and natrn.claim_gmm_flg  = vn.claim_gmm_flg 		 
		 and ( vn.ifrs17_var_current = pp.ifrs17_var_current or vn.ifrs17_var_future = pp.ifrs17_var_future ) )	 
		 and natrn.post_dt  between v_control_start_dt and v_control_end_dt;

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP04 V1: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	  
 	raise notice 'STEP04 % : % row(s) - V1 Refund',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	
 	-- ================================== V1,V2 บันทึกค้างจ่ายสินไหม HQ =================================================================--
 	BEGIN		 
		-- 2. บันทึกค้างจ่ายสินไหม HQ
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select aa.ref_1 , aa.adth_rk , aa.adth_pay_rk , aa.natrn_x_dim_rk
			, aa.branch_cd , aa.sap_doc_type , aa.reference_header
			, aa.post_dt , aa.claim_dt , aa.accident_dt , aa.claim_ok_dt
			, aa.doc_dt , aa.doc_type , aa.doc_no , aa.accode , aa.dc_flg , aa.actype
			, aa.posting_claim_amt , aa.claim_amt
			, aa.posting_claim_pay_amt , aa.claim_pay_amt
			, aa.system_type , aa.transaction_type , aa.premium_type
			, aa.policy_no , aa.plan_cd , aa.rider_cd
			, aa.pay_status_cd , aa.pay_dt , aa.pay_by_channel
			, aa.sum_natrn_amt , aa.posting_sap_amt , aa.posting_proxy_amt
			, aa.sales_id , aa.sales_struct_n
			, aa.selling_partner , aa.distribution_mode , aa.product_group , aa.product_sub_group
			, aa.rider_group , aa.product_term , aa.cost_center
			, aa.adth_dim_txt , aa.source_filename
			, aa.group_nm , aa.subgroup_nm , aa.subgroup_desc
			, aa.variable_cd
			, vn.variable_name as variable_nm
			, aa.nadet_detail , aa.org_branch_cd , aa.org_submit_no , aa.submit_no
			, aa.section_order_no , aa.is_section_order_no
			, aa.for_branch_cd , aa.event_type
			, aa.claim_gmm_flg , aa.claim_gmm_desc
			, aa.policy_type , aa.effective_dt , aa.issued_dt
			, aa.ifrs17_channelcode , aa.ifrs17_partnercode , aa.ifrs17_portfoliocode
			, aa.ifrs17_portid , aa.ifrs17_portgroup
			, vn.is_duplicate_variable
			, vn.duplicate_fr_variable_nm
		from ( 
		select  gl.ref_1 as ref_1 ,adth.adth_gl_rk as adth_rk  ,null::bigint as adth_pay_rk , null::bigint natrn_x_dim_rk  
			,adth.branch_cd 
			,gl.sap_doc_type ,gl.reference_header,gl.post_dt 
			,claim.receive_dt as claim_dt 
			,claim.accident_dt as accident_dt 
			,claim.claimok_dt  as claim_ok_dt 
			,coalesce(adth.doc_dt,gl.doc_dt ) as doc_dt ,adth.doc_type,adth.doc_no,gl.sap_acc_cd as accode,adth.dc_flg,null::varchar as actype 
			,case when adth.posting_claim_amt is null then 
				case when adth.dc_flg = acc.dc_flg then adth.claim_amt*acc.inflow_flg else adth.claim_amt*acc.inflow_flg*-1 end  
				else adth.posting_claim_amt end  posting_claim_amt	
			,adth.claim_amt
			,case when adth.posting_claim_amt is null then 
				case when adth.dc_flg = acc.dc_flg then adth.claim_amt*acc.inflow_flg else adth.claim_amt*acc.inflow_flg*-1 end  
				else adth.posting_claim_amt end as posting_claim_pay_amt 
			,adth.claim_amt  as claim_pay_amt
			,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
			,adth.policy_no,adth.plan_cd, adth.rider_cd,null::varchar as pay_status_cd ,null::date pay_dt ,null::varchar pay_by_channel
			,0::numeric as sum_natrn_amt  
			,gl.posting_sap_amt,gl.posting_proxy_amt 
			,adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
			,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
			,adth.adth_dim_txt
			,adth.source_filename , 'gl-claim' as  group_nm  , 'claim-Accrued Service' as subgroup_nm ,  'บันทึกค้างจ่ายสินไหม HQ' as subgroup_desc  
	 		,'V1'::varchar  as variable_cd   --Oil 15Jul2022 : ย้ายไป V1 [SEQ NO :5]
			--, case when date_part('day',gl.post_dt ) between 1 and 18 then 'V1'
	 			 --  when date_part('day',gl.post_dt ) >= 19 then 'V2' end as variable_cd 
			, null::varchar as nadet_detail 
			, adth.org_branch_cd
			, null::varchar as org_submit_no
			, null::varchar as submit_no
			, adth.section_order_no
			, 0 as is_section_order_no
			,adth.for_branch_cd
			,coa.event_type  
			,coa.claim_gmm_flg,coa.claim_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup
			from stag_a.stga_ifrs_nac_txn_step02 gl  
			inner join dds.ifrs_adth_gl_chg adth   
			on ( gl.ref_1 = adth.ref1_header 
			and gl.gl_dim_txt = adth.adth_dim_txt
			and gl.sap_acc_cd = adth.accode )
			left outer join stag_a.stga_sum_ifrs_claim_trans claim 
			on (adth.section_order_no = claim.section_order_no 
			and adth.policy_no = claim.policy_no )
			left outer join stag_s.ifrs_common_coa coa
			on ( gl.sap_acc_cd = coa.accode )
			left join stag_s.ifrs_common_accode  acc 
			on ( gl.sap_acc_cd = acc.account_cd  )  
			 left join  stag_s.stg_tb_core_planspec pp 
			 on ( adth.plan_cd = pp.plan_cd) 
			 left outer join dds.tl_acc_policy_chg pol  
			 on (  adth.policy_no = pol.policy_no
			 and adth.plan_cd = pol.plan_cd
			 and gl.post_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
			 where coa.event_type = 'Claim' 
			 and gl.sap_doc_type = 'KI'
			 --and adth.doc_no like '5%' -- 14Mar2022 - K.Ked cf filter like 5%'/MIS >> all of doc_no = '0000000'
			 and gl.reference_header in ( 'CCLAIM','GCLAIM')
			 and gl.post_dt between  v_control_start_dt and v_control_end_dt 
			 ) as aa 
			 inner join stag_s.ifrs_common_variable_nm vn
			 on ( vn.event_type = 'Claim'
			 and vn.variable_cd = aa.variable_cd
			 and vn.claim_gmm_flg  = aa.claim_gmm_flg 
			 );
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP05 V1,V2 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	  
 	raise notice 'STEP05 % : % row(s) - V1,V2 บันทึกค้างจ่ายสินไหม HQ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 
 	-- ================================== V1,V2 Fax claim =================================================================--
 	BEGIN	
	 	
	   --31JUl2025 Nuttadet.o : add temp table 
	   truncate table stag_a.stga_ifrs_claim_trans_temp;
	
	   insert into  stag_a.stga_ifrs_claim_trans_temp
	   select section_order_no , policy_no , rider_type 
					, receive_dt as receive_dt 
					,max(accident_dt) as accident_dt 
					,max(claimok_dt)  as claimok_dt 
					,max(pay_dt) as pay_dt  
					from  stag_a.stga_ifrs_claim_trans 
					where trim(section_order_no) <> ''
					group by section_order_no , policy_no ,rider_type, receive_dt;	 	 	
	 	
		--  V1,V2 Fax claim
		insert into stag_a.stga_ifrs_nac_txn_step05_claim   	 
		select aa.ref_1 , aa.adth_rk , aa.adth_pay_rk , aa.natrn_x_dim_rk
			, aa.branch_cd , aa.sap_doc_type , aa.ref1_header
			, aa.post_dt , aa.claim_dt , aa.accident_dt , aa.claim_ok_dt
			, aa.doc_dt , aa.doc_type , aa.doc_no , aa.accode , aa.dc_flg , aa.actype
			, aa.posting_claim_amt , aa.claim_amt
			, aa.posting_claim_pay_amt , aa.claim_pay_amt
			, aa.system_type , aa.transaction_type , aa.premium_type
			, aa.policy_no , aa.plan_cd , aa.rider_cd
			, aa.pay_status_cd , aa.pay_dt , aa.pay_by_channel
			, aa.sum_natrn_amt , aa.posting_sap_amt , aa.posting_proxy_amt
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
			, aa.claim_gmm_flg , aa.claim_gmm_desc
			, aa.policy_type , aa.effective_dt , aa.issued_dt
			, aa.ifrs17_channelcode , aa.ifrs17_partnercode , aa.ifrs17_portfoliocode
			, aa.ifrs17_portid , aa.ifrs17_portgroup
			, vn.is_duplicate_variable
			, vn.duplicate_fr_variable_nm
		from ( 		 
			 select  gl.ref1_header as ref_1 ,adth.adth_fax_claim_rk as adth_rk  ,null::bigint as adth_pay_rk , null::bigint natrn_x_dim_rk  
			,adth.branch_cd 
			,gl.doc_type as sap_doc_type ,left(gl.ref1_header ,6) as ref1_header ,gl.posting_date_dt as post_dt 
			,claim.receive_dt as claim_dt 
			,claim.accident_dt as accident_dt 
			,claim.claimok_dt  as claim_ok_dt 
			,coalesce(adth.doc_dt,gl.doc_dt ) as doc_dt ,adth.doc_type,adth.doc_no,gl.account_no as accode,adth.dc_flg,null::varchar as actype 
			,adth.posting_claim_amt	,adth.claim_amt
			,adth.posting_claim_amt as posting_claim_pay_amt, adth.claim_amt  as claim_pay_amt
			,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
			,adth.policy_no,adth.plan_cd, adth.rider_cd,null::varchar as pay_status_cd ,claim.pay_dt pay_dt ,null::varchar pay_by_channel
			,0::numeric as sum_natrn_amt  
			,gl.posting_sap_amt,gl.posting_sap_amt as posting_proxy_amt
			,adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
			,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
			,adth.adthtran_dim_txt as adth_dim_txt
			,adth.source_filename , 'sap-fax claim' as  group_nm  , 'sap-fax claim' as subgroup_nm ,  'Fax Claim' as subgroup_desc 
		 	,'V1'::varchar  as variable_cd   --Oil 15Jul2022 : ย้ายไป V1 [SEQ NO :6]
			--, case when  date_part('day',gl.posting_date_dt ) between 1 and 18 then 'V1'  
		 			--when  date_part('day',gl.posting_date_dt ) >= 19 then 'V2' end as variable_cd     
			, null::varchar as nadet_detail 
			, adth.org_branch_cd
			, null::varchar as org_submit_no
			, null::varchar as submit_no
			, adth.section_order_no
			, 1 as is_section_order_no
			, adth.branch_cd for_branch_cd
			, coa.event_type  
			, coa.claim_gmm_flg,coa.claim_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup  
			from stag_a.stga_ifrs_nac_txn_step01 gl  
			inner join  dds.ifrs_adth_payment_api_chg adth  
			on ( gl.ref1_header = adth.ref_header 
			and gl.account_no  = adth.accode 
			and gl.sap_dim_txt = adth.adthtran_dim_txt)
--			left outer join -- stag_a.stga_sum_ifrs_claim_trans claim 
--			(select section_order_no , policy_no , rider_type 
--				, receive_dt as receive_dt 
--				,max(accident_dt) as accident_dt 
--				,max(claimok_dt)  as claimok_dt 
--				,max(pay_dt) as pay_dt
--				from  stag_a.stga_ifrs_claim_trans 
--				where trim(section_order_no) <> ''
--				group by section_order_no , policy_no ,rider_type, receive_dt )	 claim
			left outer join stag_a.stga_ifrs_claim_trans_temp claim 
			on (adth.section_order_no = claim.section_order_no 
			and adth.policy_no = claim.policy_no
			and adth.rider_cd = claim.rider_type)
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
			where coa.event_type = 'Claim'    
			 and gl.doc_type  = 'KI'
			 and org_branch_cd in ( 'CCL','GCL' )
			 and gl.posting_date_dt between  v_control_start_dt and v_control_end_dt 
			 ) as aa 
			 inner join stag_s.ifrs_common_variable_nm vn
			 on ( vn.event_type = 'Claim'  
			 and vn.variable_cd = aa.variable_cd 
			 and vn.claim_gmm_flg  = aa.claim_gmm_flg 
			 ) ;
			
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP06 V1,V2 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	  
 	raise notice 'STEP06 % : % row(s) - V1,V2 Fax claim',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	-- Oil 2022Sep08 : K.Ball cf เพิ่ม Source fax claim บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน
 	BEGIN		 
		-- บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน 
		insert into stag_a.stga_ifrs_nac_txn_step05_claim 
 		select  natrn.ref_1 as ref_1 , adth.adth_fax_claim_rk as adth_rk ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk   
                ,adth.branch_cd 
                ,natrn.sap_doc_type,natrn.reference_header::varchar(100) ,natrn.post_dt
                , adth.receive_dt as claim_dt
                , adth.accident_dt as accident_dt
                , adth.claim_ok_dt 
                ,adth.doc_dt,natrn.doc_type,adth.doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
                ,adth.posting_claim_amt as posting_claim_amt, adth.claim_amt  as claim_amt 
                ,adth.posting_claim_amt as posting_claim_pay_amt, adth.claim_amt  as claim_pay_amt  
                ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
                ,adth.policy_no,adth.plan_cd, adth.rider_cd  
                ,null::varchar as pay_status_cd ,null::date as  pay_dt ,null::varchar as pay_by_channel
                ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
                ,adth.sales_id,adth.sales_struct_n    as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
                ,adthtran_dim_txt as nac_dim_txt  
                ,adth.source_filename::varchar(100) , 'natrn-fax claim'::varchar(100) as  group_nm  
                , 'claim'::varchar(100)  as subgroup_nm 
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
                ,natrn.claim_gmm_flg,natrn.claim_gmm_desc 
                ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
                ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
                ,pp.ifrs17_portid ,pp.ifrs17_portgroup
                ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm  
                from  stag_a.stga_ifrs_nac_txn_step04  natrn
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
                 on ( vn.event_type = 'Claim'
                 and vn.variable_cd = 'V1'
                 and vn.claim_gmm_flg = natrn.claim_gmm_flg 
                 )
                left outer join dds.tl_acc_policy_chg pol  
                 on (  adth.policy_no = pol.policy_no
                 and  adth.plan_cd = pol.plan_cd
                 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
                 where  natrn.event_type = 'Claim' 
                 and adth.pay_type <> '2' 
                 and adth.branch_cd = '000'  
                 and natrn.post_dt between  v_control_start_dt and v_control_end_dt ;
                
  		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP07 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
		END;	  
 		raise notice 'STEP07 % : % row(s) - V1 บันทึกโอนด่วนสินไหมมรณกรรม/ทดแทน',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  
  
  	---=================================== V1 ACCRUAL_N1 ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link ==============================
 	-- 23Feb2024 Narumol W.: [ITRQ#67020753] เพิ่ม source accrued จากการกลับรายการ N1 งาน Unit link
	BEGIN	
 		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select accru.ref_1 as ref_1,0::int as accrual_n1_rk,null::int as adth_pay_rk, null::int as natrn_x_dim_rk 
		, accru.branch_cd ,null::varchar sap_doc_type ,accru.ref_1 reference_header,accru.doc_dt as post_dt   
		, claim.receive_dt as claim_dt 
		, claim.accident_dt 
		, claim.claimok_dt as claim_ok_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt , accru.accru_amt  
		, accru.posting_accru_amount as posting_amt , accru.accru_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::varchar as pay_status_cd,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, 0::int as sum_natrn_amt ,0::int posting_sap_amt,0::int posting_proxy_amt    
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group
		,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'dds.ifrs_accrual_n1_chg' as source_filename , 'accrual_n1' as  group_nm
		,'ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link'::varchar(100) as subgroup_nm , 'ข้อมูลการบันทึกบัญชีกลับรายการ N1 งาน Unit link'  as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, ''::varchar as nadet_detail ,accru.org_branch_cd 
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,accru.for_branch for_branch_cd
		,coa.event_type  
		,coa.claim_gmm_flg,coa.claim_gmm_desc 		
		,pol.policy_type,pol.effective_dt ,pol.issued_dt  
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from dds.ifrs_accrual_n1_chg accru      
		left join  stag_s.ifrs_common_coa coa 
		on ( coa.event_type = 'Claim'
		and accru.accode = coa.accode  )
		left outer join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( trim(accru.ref_no) = claim.section_order_no
			and accru.policy_no = claim.policy_no)
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( accru.plan_code = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = coa.claim_gmm_flg  )  
		 left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no 
		 and accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )   
		 where accru.doc_dt between  v_control_start_dt and v_control_end_dt ; 
		
 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP07 V1 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
		END;	  
 		raise notice 'STEP07 % : % row(s) - V1 ACCRUAL_N1',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 	 
 		-- ================================== V2 =================================================================--
 --Oil 15Jul2022 : ย้ายไป V1 [SEQ NO : 7] ยกเลิก
 /*	BEGIN		 
		-- 2. บันทึกค้างจ่ายสินไหม HQ
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  gl.ref_1 as ref_1 ,adth.adth_gl_rk as adth_rk  ,null::bigint as adth_pay_rk , null::bigint natrn_x_dim_rk  
		,adth.branch_cd 
		,gl.sap_doc_type ,gl.reference_header,gl.post_dt 
		,claim.receive_dt as claim_dt 
		,claim.accident_dt as accident_dt 
		,claim.claimok_dt  as claim_ok_dt 
		,coalesce(adth.doc_dt,gl.doc_dt ) as doc_dt ,adth.doc_type,adth.doc_no,gl.sap_acc_cd as accode,adth.dc_flg,null::varchar as actype 
		,case when adth.posting_claim_amt is null then 
				case when adth.dc_flg = acc.dc_flg then adth.claim_amt*acc.inflow_flg else adth.claim_amt*acc.inflow_flg*-1 end  
				else adth.posting_claim_amt end posting_claim_amt	
		,adth.claim_amt
		,case when adth.posting_claim_amt is null then 
				case when adth.dc_flg = acc.dc_flg then adth.claim_amt*acc.inflow_flg else adth.claim_amt*acc.inflow_flg*-1 end  
				else adth.posting_claim_amt end as posting_claim_pay_amt
		, adth.claim_amt  as claim_pay_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd,null::varchar as pay_status_cd ,null::date pay_dt ,null::varchar pay_by_channel
		,0::numeric as sum_natrn_amt  
		,gl.posting_sap_amt,gl.posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
		,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,adth.adth_dim_txt
		,adth.source_filename , 'gl-claim' as  group_nm  , 'claim-Accrued Service' as subgroup_nm ,  'บันทึกค้างจ่ายสินไหม HQ' as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm     
		, null::varchar as nadet_detail 
		, adth.org_branch_cd
		, null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,adth.for_branch_cd
		,coa.event_type  
		,coa.claim_gmm_flg,coa.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from stag_a.stga_ifrs_nac_txn_step02 gl  
		inner join dds.ifrs_adth_gl_chg adth    
		on ( gl.ref_1 = adth.ref1_header 
		and gl.gl_dim_txt = adth.adth_dim_txt
		and gl.sap_acc_cd = adth.accode )
		left outer join stag_a.stga_sum_ifrs_claim_trans claim 
		on (adth.section_order_no = claim.section_order_no 
			and adth.policy_no = claim.policy_no )
		left outer join stag_s.ifrs_common_coa coa
		on ( gl.sap_acc_cd = coa.accode )
		left join stag_s.ifrs_common_accode  acc 
		on ( gl.sap_acc_cd = acc.account_cd  )  
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V2'
		 and vn.claim_gmm_flg  = coa.claim_gmm_flg 
		 )
		 left outer join stag_a.stga_tl_acc_policy pol  
		 on ( adth.policy_no = pol.policy_no		 
			 and adth.plan_cd = pol.plan_cd)
		where coa.event_type = 'Claim' 
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_claim  txn
						where txn.adth_rk = adth.adth_gl_rk and txn.variable_cd in ( 'V1','V2') )
		and gl.post_dt between  v_control_start_dt and v_control_end_dt;
		
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP07 V2 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	  
 	raise notice 'STEP07 % : % row(s) - V2 บันทึกค้างจ่ายสินไหม HQ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 */
  --Oil 08Sep2022 : V2 ย้ายไป V1 [SEQ NO : 8]
 	BEGIN
	 	-- 2. บันทึกค้างจ่ายสินไหม Branch
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  natrn.ref_1 as ref_1 , adth.adth_rk ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		, adth.receive_dt  as claim_dt
		, adth.accident_dt as accident_dt
		, adth.claim_ok_dt 
		,adth.ref_doc_dt doc_dt,natrn.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.posting_claim_amt as posting_claim_amt ,adth.claim_amt 
		,adth.posting_claim_amt as posting_claim_pay_amt ,adth.claim_amt  claim_pay_amt
		--,adth.posting_claim_pay_amt as posting_claim_pay_amt, adth.claim_pay_amt  as claim_pay_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd 
		,null::varchar pay_status_cd,null::date pay_dt,null::varchar as pay_by_channel
		--, adth.pay_status_cd ,adth.pay_dt as  pay_dt ,adth.method_type_cd as pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as adth_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'บันทึกค้างจ่ายสินไหม Branch' as subgroup_nm ,  'บันทึกค้างจ่ายสินไหม Branch' as subgroup_desc  
 		, vn.variable_cd
		, vn.variable_name as variable_nm   
		, natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc   
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04  natrn
		 inner join  dds.oic_adth_chg  adth --dds.ifrs_adth_pay_chg adth
		 on (   adth.branch_cd = natrn.branch_cd  
				and adth.ref_doc_dt = natrn.doc_dt  
				 and adth.ref_doc_no = natrn.doc_no 
				 and adth.accode = natrn.accode 
				 and adth.dc_flg = natrn.dc_flg   
				and adth.adth_dim_txt = natrn.natrn_dim_txt )
			--and coalesce(adth.pay_status_cd,'') not in ( 'N','C','R')) -- Log.168,129
		 left join stag_s.ifrs_common_accode  acc 
		 on ( adth.accode = acc.account_cd  ) 
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
		 )
		 left outer join stag_a.stga_tl_acc_policy pol  
		 on ( adth.policy_no = pol.policy_no 
			 and adth.plan_cd = pol.plan_cd)
		 where natrn.branch_cd <> '000' -- Log.83
		and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_claim  txn
						where txn.adth_rk = adth.adth_rk and txn.variable_cd in ( 'V1','V2') and group_nm = 'natrn-claim') 
		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP08 V2 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	   
	raise notice 'STEP08 % : % row(s) - V2 บันทึกค้างจ่ายสินไหม Branch',clock_timestamp()::varchar(19),v_affected_rows::varchar;
  
 	-- ===================================== V3 ==============================================================--
	-- หักล้างสินไหมอนุมัติรอจ่าย และ flag Cancel (V4)
 	BEGIN		 
		 -- 3. บันทึกจ่ายสินไหมจากสินไหมอนุมัติรอจ่าย  Move V2 ของเดือนที่แล้ว
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select ytd.ref_1, ytd.adth_rk, ytd.adth_pay_rk, ytd.natrn_x_dim_rk, ytd.branch_cd, ytd.sap_doc_type, ytd.reference_header
                , v_control_end_dt as post_dt
                , ytd.claim_dt, ytd.accident_dt, ytd.claim_ok_dt, ytd.doc_dt, ytd.doc_type, ytd.doc_no, ytd.accode, ytd.dc_flg, ytd.actype
                , ytd.posting_claim_amt*-1, ytd.claim_amt
                , ytd.posting_claim_pay_amt*-1, ytd.claim_pay_amt, ytd.system_type, ytd.transaction_type
                , ytd.premium_type, ytd.policy_no, ytd.plan_cd, ytd.rider_cd, ytd.pay_status_cd, ytd.pay_dt, ytd.pay_by_channel
                , ytd.sum_natrn_amt, ytd.posting_sap_amt, ytd.posting_proxy_amt, ytd.sales_id, ytd.sales_struct_n
                , ytd.selling_partner, ytd.distribution_mode, ytd.product_group, ytd.product_sub_group, ytd.rider_group, ytd.product_term, ytd.cost_center, ytd.nac_dim_txt
                , ytd.source_filename, ytd.group_nm, ytd.subgroup_nm, ytd.subgroup_desc
                , 'V3' as variable_cd, 'ACTUAL_BEL_U_PDDUM' as variable_nm
                , ytd.nadet_detail, ytd.org_branch_cd, ytd.org_submit_no, ytd.submit_no, ytd.section_order_no, ytd.is_section_order_no
                , ytd.for_branch_cd, ytd.event_type, ytd.claim_gmm_flg, ytd.claim_gmm_desc
                , ytd.policy_type, ytd.effective_dt, ytd.issued_dt
                , ytd.ifrs17_channelcode, ytd.ifrs17_partnercode, ytd.ifrs17_portfoliocode, ytd.ifrs17_portid, ytd.ifrs17_portgroup
                , ytd.is_duplicate_variable, ytd.duplicate_fr_variable_nm 
                from dds.ifrs_variable_claim_ytd ytd
                where sap_doc_type = 'KI'
                and variable_cd = 'V2'
                and ytd.dc_flg = 'D' 	-- 21Feb2022 : CF By K.Ball Filter 'D'
                and control_dt between date_trunc('month', v_control_start_dt - interval '1 month')::date and  (v_control_end_dt - interval '1 month')::date;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP10 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP10 % : % row(s) - V3 บันทึกจ่ายสินไหมจากสินไหมอนุมัติรอจ่าย',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
	-- 2. adthactrf  (reject โอนเงิน)  3. demanddraft (reject draft)
 	-- Oil 2022Jul15 : ยกเลิกV3 เปลี่ยนไปใช้ Ending Balance จาก Data Smile 
 /*
 	BEGIN		 
	 	
	 	-- Log.183.3 : Fixed deduplicate reject_amt
		with adth_pay as ( 
		select *
		 , ROW_NUMBER ( )  OVER (PARTITION BY reject_doc_dt,reject_doc_type,reject_doc_no ORDER BY reject_doc_dt,reject_doc_type,reject_doc_no,seq_no desc   ) as _RK  
		from  dds.ifrs_adth_pay_chg 
		where reject_doc_dt is not null 
		and  reject_doc_dt between  v_control_start_dt and v_control_end_dt  
		) 

		 -- 3.บันทึกจ่ายสินไหมจากสินไหมอนุมัติรอจ่าย (ล้าง uppaided approved claim, LIC)
		insert into stag_a.stga_ifrs_nac_txn_step05_claim   
		select  natrn.ref_1 as ref_1 , adth.adth_rk  ,adth.adth_pay_rk , natrn.natrn_x_dim_rk  
            ,natrn.branch_cd 
            ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
            , adth.receive_dt  as claim_dt
            , adth.accident_dt as accident_dt
            , adth.claim_ok_dt 
            ,adth.reject_doc_dt doc_dt
            ,adth.reject_doc_type doc_type
            ,adth.reject_doc_no doc_no
            ,natrn.accode,natrn.dc_flg,null::varchar as actype  
            ,adth.reject_total_amt*-1 as posting_claim_amt ,adth.reject_total_amt 
            ,adth.reject_total_amt*-1 as posting_claim_pay_amt ,adth.reject_total_amt as claim_pay_amt  
            ,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
            ,adth.policy_no,adth.plan_cd, adth.rider_cd , adth.pay_status_cd ,adth.pay_dt as  pay_dt ,adth.method_type_cd as pay_by_channel
            ,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
            ,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
            ,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as adth_dim_txt  
            ,adth.source_filename , 'natrn-claim' as  group_nm  , 'Reject' as subgroup_nm ,  'Reject โอนเงิน, demand draft' as subgroup_desc  
            , vn.variable_cd
            , vn.variable_name as variable_nm   
            , natrn.detail as nadet_detail 
            , adth.org_branch_cd,null::varchar as org_submit_no
            , null::varchar as submit_no
            , adth.section_order_no
            , 0 as is_section_order_no
            ,natrn.for_branch_cd
            ,natrn.event_type  
            ,natrn.claim_gmm_flg,natrn.claim_gmm_desc   
            ,pol.policy_type ,pol.effective_dt ,pol.issued_dt
            ,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
            ,pp.ifrs17_portid ,pp.ifrs17_portgroup
            ,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
            from  stag_a.stga_ifrs_nac_txn_step04  natrn
             inner join adth_pay adth --  dds.ifrs_adth_pay_chg adth 
             on ( adth._RK= 1  and adth.reject_doc_dt = natrn.doc_dt  
              	and adth.reject_doc_type = natrn.doc_type
				and adth.reject_doc_no = natrn.doc_no  ) 
             left join stag_s.ifrs_common_accode  acc 
             on ( adth.accode = acc.account_cd  ) 
             left join  stag_s.stg_tb_core_planspec pp 
             on ( adth.plan_cd = pp.plan_cd)
             inner join stag_s.ifrs_common_variable_nm vn
             on ( vn.event_type = 'Claim'
             and vn.variable_cd = 'V3'
             and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
             )
             left outer join stag_a.stga_tl_acc_policy pol  
             on ( adth.policy_no = pol.policy_no 
              and adth.plan_cd = pol.plan_cd)
             where natrn.dc_flg = 'C'    
             and natrn.post_dt between  v_control_start_dt and v_control_end_dt; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP10 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP11 % : % row(s) - V3 บันทึกจ่ายสินไหมจากสินไหมอนุมัติรอจ่าย',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	------------------------
 	-- 3.  Unpaid approved claim 
 	-- 18Apr22 Change Condition to SAP : Dr. 
	BEGIN	      
	 	 insert into stag_a.stga_ifrs_nac_txn_step05_claim
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt 
		, doc_dt ,accode , dc_flg , claim_amt , posting_claim_amt ,claim_dt
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		,source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		,variable_cd ,variable_nm, event_type ,claim_gmm_flg ,claim_gmm_desc) 
		 select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt ,account_no as sap_acc_cd ,dc_flg ,local_sap_amt ,posting_sap_amt , doc_dt as claim_dt
		, posting_sap_amt ,posting_sap_amt 
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,term_cd ,cost_center,sap_dim_txt 
		,'gl_proxy' as source_filename                 
		,'sap'::varchar(100) as  group_nm , 'สินไหมอนุมัติรอจ่าย'::varchar(100) as subgroup_nm , 'สินไหมอนุมัติรอจ่าย' ::varchar(100) as subgroup_desc  
		, vn.variable_cd , vn.variable_name 
		, coa.event_type , coa.claim_gmm_flg ,coa.claim_gmm_desc 
		from stag_a.stga_ifrs_nac_txn_step01 gl
		inner join stag_s.ifrs_common_coa coa 
		on ( gl.account_no = coa.accode  )  
		inner join stag_s.ifrs_common_variable_nm vn
		on ( vn.event_type = 'Claim'
		and vn.variable_cd = 'V3'
		and coa.claim_gmm_flg = vn.claim_gmm_flg)
		where gl.dc_flg = 'D' 
		and posting_date_dt  between v_control_start_dt and v_control_end_dt; 
         
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP11 V3 Unpaid approved claim : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP11 % : % row(s) - V3 SAP',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
    
 	--V3 กลับรายการปรับปรุง สินไหมอนุมัติรอจ่าย
 	BEGIN	      
	 	
		insert into stag_a.stga_ifrs_nac_txn_step05_claim    
		select  natrn.ref_1 as ref_1 ,natrn.natrn_x_dim_rk ,null::bigint adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,natrn.doc_dt as claim_dt
		,null::date as accident_dt   
		,null::date as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt*-1 as posting_amt , natrn.natrn_amt  
		,natrn.posting_natrn_amt*-1 as posting_amt , natrn.natrn_amt  
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type 
		,coalesce(natrn.nadet_policyno,pol.policy_no ) as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd ) as plan_cd 
		,null::varchar as rider_cd 
		,null::varchar as pay_status_cd 
		,null::date as pay_dt ,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename   
		, 'natrn'::varchar(100) as  group_nm ,'Reverse unpaid approve claim'::varchar(100) as subgroup_nm , 'กลับรายการปรับปรุง สินไหมอนุมัติรอจ่าย'::varchar(100) as subgroup_desc    
		, vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 
		,pol.policy_type,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04 natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )   
		left join  stag_a.stga_tl_acc_policy pol 
		on ( natrn.nadet_policyno = pol.policy_no ) 
		left join  stag_s.stg_tb_core_planspec pp 
		 on (coalesce(natrn.plan_cd,pol.plan_cd) = pp.plan_cd)   
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V3'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg  
		 )			 
		 where natrn.is_reverse = 1  
		 and natrn.dc_flg = 'D'  
  		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt ; 
		 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP12 V3 Unpaid approved claim : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP12 % : % row(s) - VX Manual',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 	
  	-- ================================== V3 สินไหมอนุมัติรอจ่าย - HQ =================================================================-- 
 	-- V3 สินไหมอนุมัติรอจ่าย - HQ 22Mar2022 - New Requirement from K.Ked

	BEGIN
	 	-- 2. สินไหมอนุมัติรอจ่าย - HQ
		insert into stag_a.stga_ifrs_nac_txn_step05_claim   
		select  natrn.ref_1 as ref_1 , adth.adth_rk  ,null::bigint as adth_pay_rk , natrn.natrn_x_dim_rk  
		,adth.branch_cd 
		,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		, adth.receive_dt  as claim_dt
		, adth.accident_dt as accident_dt
		, adth.claim_ok_dt 
		,adth.ref_doc_dt doc_dt,natrn.doc_type,adth.ref_doc_no doc_no,natrn.accode,adth.dc_flg,null::varchar as actype  
		,adth.posting_claim_amt as posting_claim_amt ,adth.claim_amt 
		,adth.posting_claim_amt as posting_claim_pay_amt ,adth.claim_amt  claim_pay_amt
		--,adth.posting_claim_pay_amt as posting_claim_pay_amt, adth.claim_pay_amt  as claim_pay_amt
		,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
		,adth.policy_no,adth.plan_cd, adth.rider_cd 
		,null::varchar pay_status_cd,null::date pay_dt,null::varchar as pay_by_channel
		,natrn.posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,adth.sales_id,adth.sales_struct_n as sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
		,concat(adth.selling_partner,adth.distribution_mode,adth.product_group,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center) as adth_dim_txt  
		,adth.source_filename , 'natrn-claim' as  group_nm  , 'สินไหมอนุมัติรอจ่าย - HQ' as subgroup_nm ,  'สินไหมอนุมัติรอจ่าย - HQ' as subgroup_desc  
 		, vn.variable_cd
		, vn.variable_name as variable_nm   
		, natrn.detail as nadet_detail 
		, adth.org_branch_cd,null::varchar as org_submit_no
		, null::varchar as submit_no
		, adth.section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc   
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from  stag_a.stga_ifrs_nac_txn_step04  natrn
		 inner join  dds.oic_adth_chg  adth --dds.ifrs_adth_pay_chg adth
		 on (   adth.branch_cd = natrn.branch_cd  
				and adth.ref_doc_dt = natrn.doc_dt  
				 and adth.ref_doc_no = natrn.doc_no 
				 and adth.accode = natrn.accode 
				 and adth.dc_flg = natrn.dc_flg   
				and adth.adth_dim_txt = natrn.natrn_dim_txt )
			--and coalesce(adth.pay_status_cd,'')  not in ( 'N','C','R')) -- Log.168,129
		 left join stag_s.ifrs_common_accode  acc 
		 on ( adth.accode = acc.account_cd  ) 
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( adth.plan_cd = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V3'
		 and vn.claim_gmm_flg = natrn.claim_gmm_flg 
		 )
		 left outer join stag_a.stga_tl_acc_policy pol  
		 on ( adth.policy_no = pol.policy_no 
			 and adth.plan_cd = pol.plan_cd)
		 where natrn.branch_cd = '000' 
		 and natrn.doc_no not like '5%'
		 and natrn.dc_flg = 'C'
		 and natrn.branch_cd = '000'    
		 and not exists ( select 1 from  stag_a.stga_ifrs_nac_txn_step05_claim  txn
							where txn.adth_rk = adth.adth_rk and txn.variable_cd in ( 'V1','V2','V3' ) ) 
		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP13 V3 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	   
	raise notice 'STEP13 % : % row(s) - V3 สินไหมอนุมัติรอจ่าย - HQ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
*/
 	-- ===================================== V5 ==============================================================--
  	-- Log.116  Data Diff 
	-- 18Jan2023 Narumol W.: Log.227 Use natrn.ref_1 instead of accru.ref1

  	begin -- select * from  stag_a.stga_ifrs_nac_txn_step05_claim  
		 -- 3. บันทึกค้างจ่ายสินไหม ณ สิ้นเดือน
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk ,null::bigint as adth_pay_rk,  natrn.natrn_x_dim_rk  
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt   
		, claim.receive_dt as claim_dt 
		, claim.accident_dt 
		, claim.claimok_dt as claim_ok_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt , accru.accru_amt /*27Sep2021 Requirement แบบไม่กลับขา*/
		, accru.posting_accru_amount as posting_amt , accru.accru_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::varchar as pay_status_cd,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accruedac_current' as subgroup_nm , 'V5-บันทึกค้างจ่ายสินไหม ณ สิ้นเดือน'  as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,accru.org_branch_cd 
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 		
		,pol.policy_type,pol.effective_dt ,pol.issued_dt  
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.accode = accru.accode
		and natrn.dc_flg = accru.dc_flg 
		and natrn.natrn_dim_txt =  accru.accru_dim_txt)
		left outer join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( trim(accru.ref_no) = claim.section_order_no
			and accru.policy_no = claim.policy_no)
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( accru.plan_code = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V5'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
		 )  
		 left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no 
		 and accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 -- where natrn.claim_gmm_flg in ('1','2','3') 
		 --where accru.system_type_cd in ( '001','002','003','004','005','006','007','008','009','010','011','012')
		 where coalesce(natrn.is_accru,0) <> 1
		 and accru.doc_dt between  v_control_start_dt and v_control_end_dt ; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP14 V5 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP14 % : % row(s) - V5 บันทึกค้างจ่ายสินไหม ณ สิ้นเดือน',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
 	-- ===================================== V6 ==============================================================--
 
  	BEGIN
		 -- 6. รายการจ่ายของสินไหมค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน  	
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  natrn.ref_1 as ref_1,accru.accrual_rk  as nac_rk ,null::bigint as adth_pay_rk,  natrn.natrn_x_dim_rk  
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,v_control_end_dt as post_dt  
		, claim.receive_dt as claim_dt 
		, claim.accident_dt 
		, claim.claimok_dt as claim_ok_dt		
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		-- 03July2023 Narumol W. : [ITRQ#66062868] เปลี่ยนแปลง sign ของ CLAIM_YTD Var.6 และ BENEFIT Var.3
		, accru.posting_accru_amount as posting_amt  -- Track_chg.70  
		, accru.accru_amt
		, accru.posting_accru_amount as posting_amt , accru.accru_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::varchar as pay_status_cd,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  , 'Accruedac_current' as subgroup_nm , 'V6-รายการจ่ายของสินไหมค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน'  as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,accru.org_branch_cd
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd 
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 		
		,pol.policy_type,pol.effective_dt ,pol.issued_dt  
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_eoy_chg  accru  
		left outer join stag_s.ifrs_common_coa coa
		on ( accru.accode = coa.accode )   
		inner join stag_a.stga_ifrs_nac_txn_step04_eoy natrn  
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.accode = accru.accode
		and natrn.dc_flg = accru.dc_flg 
		and natrn.natrn_dim_txt =  accru.accru_dim_txt ) 
		left outer join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( trim(accru.ref_no) = claim.section_order_no
			and accru.policy_no = claim.policy_no) 
		 left join  stag_s.stg_tb_core_planspec pp 
		 on (accru.plan_code = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V6'
		 and vn.claim_gmm_flg  = coa.claim_gmm_flg   
		 )	
 		 left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no 
		 and accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 where not exists ( select 1 
		 					from dds.vw_ifrs_accrual_chg  cur  
		 					where accru.policy_no = cur.policy_no 
						 	and accru.accode = cur.accode
						 	and coalesce(trim(accru.plan_code),'') = coalesce(trim(cur.plan_code),'')  -- Log.215 Oil2022Sep08 : Add coalesce
						 	and coalesce(trim(accru.rider_code),'') = coalesce(trim(cur.rider_code),'') -- 13Nov2023 Narumol W. : Claim Log 401 Trim white space
						 	and accru.ref_no = cur.ref_no 
						 	and accru.transaction_type = cur.transaction_type  
						 	)  
		 and coalesce(natrn.is_accru,0) <> 1 ; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP15 V6 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP15 % : % row(s) - V6 บันทึกค้างจ่ายสินไหม ณ สิ้นเดือน',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	-- Oil 07Nov2022
	-- Claim Log 232 แบบประกันสินเชื่อสามารถหาเลข Policy ได้ ไม่ควรถูกจัดเป็น DUM_NT (Trach change 47) 
	-- STEP_01 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) <= 8
	-- 25Oct2023 Narumol W. : Add step join with policy and plan_cd after patch policy & plan_Cd 
	BEGIN	 
		
		-- Manual select * from  stag_a.stga_ifrs_nac_txn_step05_claim    
		insert into stag_a.stga_ifrs_nac_txn_step05_claim    
		select  natrn.ref_1 as ref_1  ,adth.adth_rk  ,null::bigint adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,natrn.doc_dt as claim_dt
		,claim.accident_dt   
		,claim.claimok_dt as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt , natrn.natrn_amt  
		,natrn.posting_natrn_amt as posting_amt , natrn.natrn_amt  
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type 
		,coalesce(natrn.nadet_policyno,pol.policy_no ) as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd ) as plan_cd ,null::varchar as rider_cd 
		,null::varchar pay_status_cd,null::date pay_dt,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename   
		, 'Manual'::varchar(100) as  group_nm ,'natrn'::varchar(100) as subgroup_nm , 'บันทึกจ่ายสินไหม Manual' ::varchar(100) as subgroup_desc    
		, vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 
		,pol.policy_type,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04 natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		 left join  dds.oic_adth_chg  adth  
		 on (   adth.branch_cd = natrn.branch_cd  
				and adth.ref_doc_dt = natrn.doc_dt  
				 and adth.ref_doc_no = natrn.doc_no 
				 and adth.accode = natrn.accode 
				 and adth.dc_flg = natrn.dc_flg   
				and adth.adth_dim_txt = natrn.natrn_dim_txt ) 
		 inner join dds.tl_acc_policy_chg pol  
		 on (  natrn.nadet_policyno = pol.policy_no 
		 and natrn.plan_cd  = pol.plan_cd  
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( adth.section_order_no = claim.section_order_no )
		left join  stag_s.stg_tb_core_planspec pp 
		 on ( natrn.plan_cd = pp.plan_cd)   
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
		 )			 
		 where natrn.doc_no like '5%'
		 and natrn.event_type  = 'Claim'   
  		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt ; 
  		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
  		raise notice 'STEP16.1  % : % row(s) - inner join with policy & plan_cd ',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 	
	    -- Manual select * from  stag_a.stga_ifrs_nac_txn_step05_claim    
		insert into stag_a.stga_ifrs_nac_txn_step05_claim    
		select  natrn.ref_1 as ref_1  ,adth.adth_rk  ,null::bigint adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,natrn.doc_dt as claim_dt
		,claim.accident_dt   
		,claim.claimok_dt as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt , natrn.natrn_amt  
		,natrn.posting_natrn_amt as posting_amt , natrn.natrn_amt  
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type 
		,coalesce(natrn.nadet_policyno,pol.policy_no) as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd ) as plan_cd ,null::varchar as rider_cd 
		,null::varchar pay_status_cd,null::date pay_dt,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename   
		, 'Manual'::varchar(100) as  group_nm ,'natrn'::varchar(100) as subgroup_nm , 'บันทึกจ่ายสินไหม Manual' ::varchar(100) as subgroup_desc    
		, vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 
		,pol.policy_type,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04 natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		 left join  dds.oic_adth_chg  adth --log.189
		 on (   adth.branch_cd = natrn.branch_cd  
				and adth.ref_doc_dt = natrn.doc_dt  
				 and adth.ref_doc_no = natrn.doc_no 
				 and adth.accode = natrn.accode 
				 and adth.dc_flg = natrn.dc_flg   
				and adth.adth_dim_txt = natrn.natrn_dim_txt )
			--and coalesce(adth.pay_status_cd,'') not in ('N', 'C','R')) -- Log.168,129 
		 left outer join dds.tl_acc_policy_chg pol  
		 on (  natrn.nadet_policyno = pol.policy_no 
		 and pol.policy_type not in ( 'M','G') --Log.120 
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( adth.section_order_no = claim.section_order_no )
		left join  stag_s.stg_tb_core_planspec pp 
		 on (coalesce(natrn.plan_cd,pol.plan_cd) = pp.plan_cd)   
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
		 )			 
		 where natrn.doc_no like '5%'
		 and natrn.event_type  = 'Claim'  
		 and length(nullif(trim(natrn.nadet_policyno),'')) <= 8
  		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt
  		 and not exists ( select 1 from stag_a.stga_ifrs_nac_txn_step05_claim c 
  		 					where natrn.branch_cd = c.branch_cd
  		 					and natrn.doc_dt = c.doc_dt  
  		 					and natrn.doc_type = c.doc_type  
  		 					and natrn.doc_no = c.doc_no  
  		 					and natrn.accode = c.accode
  		 					and natrn.nadet_policyno = c.policy_no
  		 					);
  		 
  		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 		raise notice 'STEP16.2  % : % row(s) - VX Manual STEP_01 length nadet_policyno <= 8',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP16 Manual STEP_01 length nadet_policyno <= 8  : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 

	-- STEP_02 policy_no Track_chg 47 length(nullif(trim(natrn.nadet_policyno),'')) > 9
	BEGIN	 
		-- 25Oct2023 Narumol W. : Add step join with policy and plan_cd after patch policy & plan_Cd 
	    -- Manual select * from  stag_a.stga_ifrs_nac_txn_step05_claim    
		insert into stag_a.stga_ifrs_nac_txn_step05_claim    
		select  natrn.ref_1 as ref_1  ,adth.adth_rk  ,null::bigint adth_pay_rk , natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt 
		,natrn.doc_dt as claim_dt
		,claim.accident_dt   
		,claim.claimok_dt as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.posting_natrn_amt as posting_amt , natrn.natrn_amt  
		,natrn.posting_natrn_amt as posting_amt , natrn.natrn_amt  
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type 
		,coalesce(natrn.nadet_policyno,pol.policy_no ) as policy_no
		,coalesce(natrn.plan_cd,pol.plan_cd ) as plan_cd ,null::varchar as rider_cd 
		,null::varchar pay_status_cd,null::date pay_dt,null::varchar as pay_by_channel
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename   
		, 'Manual'::varchar(100) as  group_nm ,'natrn'::varchar(100) as subgroup_nm , 'บันทึกจ่ายสินไหม Manual' ::varchar(100) as subgroup_desc    
		, vn.variable_cd ,vn.variable_name 
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 
		,pol.policy_type,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup  
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm   
		from  stag_a.stga_ifrs_nac_txn_step04 natrn  
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		 left join  dds.oic_adth_chg  adth --log.189
		 on (   adth.branch_cd = natrn.branch_cd  
				and adth.ref_doc_dt = natrn.doc_dt  
				 and adth.ref_doc_no = natrn.doc_no 
				 and adth.accode = natrn.accode 
				 and adth.dc_flg = natrn.dc_flg   
				and adth.adth_dim_txt = natrn.natrn_dim_txt )
			--and coalesce(adth.pay_status_cd,'') not in ('N', 'C','R')) -- Log.168,129 
		 left outer join dds.tl_acc_policy_chg pol  
		 on ( trim(natrn.nadet_policyno) = concat(trim(pol.plan_cd),trim(pol.policy_no))
		 --and pol.policy_type not in ( 'M','G') --Log.120
		 and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		 left join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( adth.section_order_no = claim.section_order_no )
		left join  stag_s.stg_tb_core_planspec pp 
		 on (coalesce(natrn.plan_cd,pol.plan_cd) = pp.plan_cd)   
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V1'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg 
		 )			 
		 where natrn.doc_no like '5%'
		 and natrn.event_type  = 'Claim'  
		 and length(nullif(trim(natrn.nadet_policyno),''))> 9 
  		 and natrn.post_dt between  v_control_start_dt and v_control_end_dt ; 
  		
  		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP16 Manual STEP_02 length nadet_policyno >  9  : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	  
 	-- ===================================== V9  ==============================================================--
 	BEGIN 
	 	
	 	insert into stag_a.stga_ifrs_nac_txn_step05_claim   
	 	select  v.ref_1, v.adth_rk, v.adth_pay_rk, v.natrn_x_dim_rk, v.branch_cd, v.sap_doc_type, v.reference_header
	 	, v.post_dt, v.claim_dt, v.accident_dt , v.claim_ok_dt
	 	, v.doc_dt, v.doc_type, v.doc_no, v.accode, v.dc_flg, v.actype
	 	, v.posting_claim_amt*(ra.ra_rate*0.01) as posting_claim_amt
	 	, v.claim_amt*(ra.ra_rate*0.01) as claim_amt
	 	, v.posting_claim_pay_amt*(ra.ra_rate*0.01) as posting_claim_pay_amt
	 	, v.claim_pay_amt*(ra.ra_rate*0.01) as claim_pay_amt
	 	, v.system_type, v.transaction_type, v.premium_type, v.policy_no, v.plan_cd, v.rider_cd, v.pay_status_cd, v.pay_dt, v.pay_by_channel
	 	, v.sum_natrn_amt  , v.posting_sap_amt ,v.posting_proxy_amt 
	 	, v.sales_id, v.sales_struct_n, v.selling_partner, v.distribution_mode
	 	, v.product_group, v.product_sub_group, v.rider_group, v.product_term, v.cost_center
	 	, v.nac_dim_txt, v.source_filename, v.group_nm, v.subgroup_nm, 'V9-บันทึกค้างจ่ายสินไหม ณ สิ้นเดือน'as subgroup_desc
	 	, 'V9' as variable_cd, vn.variable_name, v.nadet_detail
	 	, v.org_branch_cd, v.org_submit_no, v.submit_no, v.section_order_no, v.is_section_order_no, v.for_branch_cd
	 	, v.event_type, v.claim_gmm_flg, v.claim_gmm_desc, v.policy_type
	 	, v.effective_dt, v.issued_dt, v.ifrs17_channelcode, v.ifrs17_partnercode, v.ifrs17_portfoliocode
	 	, v.ifrs17_portid, v.ifrs17_portgroup
	 	-- 8Feb2023 Narumol W.: Log314
	 	, 1::int is_duplicate_variable, v.variable_nm as duplicate_fr_variable_nm
		from stag_a.stga_ifrs_nac_txn_step05_claim  v 
		inner join dds.ifrs_common_ra_rate ra  
		on ( v.event_type = ra.event_type 
		and  v.claim_gmm_flg = ra.gmm_flg )
		left outer join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V9'  )   
		-- 30Oct2023 Narumol W.: [ITRQ#66104734] Claim-ขอเพิ่มเงื่อนไขในการไม่ส่งรายการ RA ที่ไม่พบข้อมูลใน CMT  เข้า SAS Solution
	 	--where v.variable_cd = 'V5' 
	 	where left(subgroup_desc,2) = 'V5'
	 	and v.is_duplicate_variable <> 1 
  		and v.post_dt between  v_control_start_dt and v_control_end_dt ; 
	   
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP17 V9 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
   	raise notice 'STEP17 % : % row(s) - V9 Dup from V5',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
   
	-- ===================================== V10==============================================================--
  	BEGIN 
	 	
	 	insert into stag_a.stga_ifrs_nac_txn_step05_claim   
	 	select  v.ref_1, v.adth_rk, v.adth_pay_rk, v.natrn_x_dim_rk, v.branch_cd, v.sap_doc_type, v.reference_header
	 	, v.post_dt, v.claim_dt, v.accident_dt , v.claim_ok_dt
	 	, v.doc_dt, v.doc_type, v.doc_no, v.accode, v.dc_flg, v.actype
	 	, v.posting_claim_amt*(ra.ra_rate*0.01) as posting_claim_amt
	 	, v.claim_amt*(ra.ra_rate*0.01) as claim_amt
	 	, v.posting_claim_pay_amt*(ra.ra_rate*0.01) as posting_claim_pay_amt
	 	, v.claim_pay_amt*(ra.ra_rate*0.01) as claim_pay_amt
	 	, v.system_type, v.transaction_type, v.premium_type, v.policy_no, v.plan_cd, v.rider_cd, v.pay_status_cd, v.pay_dt, v.pay_by_channel
	 	, v.sum_natrn_amt  , v.posting_sap_amt ,v.posting_proxy_amt 
	 	, v.sales_id, v.sales_struct_n, v.selling_partner, v.distribution_mode
	 	, v.product_group, v.product_sub_group, v.rider_group, v.product_term, v.cost_center
	 	, v.nac_dim_txt, v.source_filename, v.group_nm, v.subgroup_nm, 'V10-รายการจ่ายของสินไหมค้างจ่ายที่กวาดมาตั้งทุกสิ้นเดือน' as subgroup_desc
	 	, 'V10' as variable_cd
	 	, vn.variable_name, v.nadet_detail
	 	, v.org_branch_cd, v.org_submit_no, v.submit_no, v.section_order_no, v.is_section_order_no, v.for_branch_cd
	 	, v.event_type, v.claim_gmm_flg, v.claim_gmm_desc, v.policy_type
	 	, v.effective_dt, v.issued_dt, v.ifrs17_channelcode, v.ifrs17_partnercode, v.ifrs17_portfoliocode
	 	, v.ifrs17_portid, v.ifrs17_portgroup
	 	-- 8Feb2023 Narumol W.: Log314
	 	, 1::int is_duplicate_variable, v.variable_nm as duplicate_fr_variable_nm
		from stag_a.stga_ifrs_nac_txn_step05_claim  v 
		inner join dds.ifrs_common_ra_rate ra  
		on ( v.event_type = ra.event_type 
		and  v.claim_gmm_flg = ra.gmm_flg )
		left outer join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V10'  )   
		-- 30Oct2023 Narumol W.: [ITRQ#66104734] Claim-ขอเพิ่มเงื่อนไขในการไม่ส่งรายการ RA ที่ไม่พบข้อมูลใน CMT  เข้า SAS Solution
	 	--where v.variable_cd = 'V6' 
		where left(subgroup_desc,2) = 'V6'
	 	and v.is_duplicate_variable <> 1  
  		and v.post_dt between  v_control_start_dt and v_control_end_dt ; 
  		
 		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP18 V10 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
	END;	
   	raise notice 'STEP18 % : % row(s) - V10 Dup from V6',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
   

 -- ===================================== V11 - sap_trial_balance	
 	-- Oil 20230516 : Add V11 stag_f.tb_sap_trial_balance >> ปรับใช้ dds.tb_sap_trial_balance	
	-- 14Sep2023 Narumol W.: [ITRQ#66094231] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
    -- 19Oct2023 Narumol W.: [ITRQ#66104655] แก้ไข sign ของ VARIABLE_YTD สำหรับข้อมูลที่ถูกส่งจาก SAP
	BEGIN	 
	    
		insert into stag_a.stga_ifrs_nac_txn_step05_claim   
		( post_dt,doc_dt,claim_dt,accode,posting_claim_amt,posting_claim_pay_amt,
	    source_filename,group_nm,subgroup_nm,subgroup_desc,
		variable_cd,variable_nm,event_type,claim_gmm_flg,claim_gmm_desc)	
		select report_date as post_dt,report_date as doc_dt,report_date as claim_dt
        , trn.gl_acct as accode 
        , trn.accumulated_balance as posting_claim_amt
        , trn.accumulated_balance as posting_claim_pay_amt
        --, 'TB_Claim' as event_type 
        , 'sap_trial_balance' as source_filename
        , 'SAP_TB' as group_nm 
        , 'Accruedac_current' as subgroup_nm 
        , 'V11-ยอดคงเหลือสินไหมอนุมัติค้างจ่าย ตาม SAP' as subgroup_desc
        --, vn.variable_cd as variable_cd 
        , coa.dummy_variable_nm as variable_cd   
        , coalesce(vn.variable_name, coa.variable_for_policy_missing)  as variable_nm
        , 'Claim' as event_type 
        ,coa.claim_gmm_flg ,coa.claim_gmm_desc
        from  dds.tb_sap_trial_balance trn -- >>  dds.tb_sap_trial_balance trn 
        inner join stag_s.ifrs_common_coa  coa
        on ( coa.event_type = 'TB_Claim'
        and coa.accode  = trn.gl_acct  ) 
        inner join stag_s.ifrs_common_variable_nm vn 
        on ( vn.event_type = 'TB_Claim'
        and vn.variable_cd = 'V11' 
        and coa.claim_gmm_flg  = vn.claim_gmm_flg ) 
        where trn.ledger = '0l'
        and report_date = v_control_end_dt;
                
     GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP18 V11 - sap_trial_balance  : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;
	
	raise notice 'STEP18 % : % row(s) - V11 - sap_trial_balance ',clock_timestamp()::varchar(19),v_affected_rows::varchar; 
 
	------------------
	-- 06Aug2024 Narumol W. : [ITRQ#67073483] เพิ่ม Source accrued-acใน Variable Unpaid Approve Claim
  	begin   
		 -- 3. รายการคืนเบี้ยสินไหมปฏิเสธค้างจ่าย ณ สิ้นเดือน
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  
		select  natrn.ref_1 as ref_1,accru.accrual_rk as nac_rk ,null::bigint as adth_pay_rk,  natrn.natrn_x_dim_rk  
		, accru.branch_cd ,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt   
		, claim.receive_dt as claim_dt 
		, claim.accident_dt 
		, claim.claimok_dt as claim_ok_dt
		, accru.doc_dt,accru.doc_type,accru.doc_no
		, accru.accode,accru.dc_flg 
		, accru.ac_type 
		, accru.posting_accru_amount as posting_amt , accru.accru_amt 
		, accru.posting_accru_amount as posting_amt , accru.accru_amt
		, accru.system_type_cd as system_type,accru.transaction_type,accru.premium_type 
		, accru.policy_no ,accru.plan_code as plan_cd, accru.rider_code as rider_cd ,null::varchar as pay_status_cd,null::date as pay_dt ,accru.pay_by as pay_by_channel
		, posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt   
		, accru.sales_id, accru.sales_struct_n, accru.selling_partner,accru.distribution_mode,accru.product_group,accru.product_sub_group,accru.rider_group,accru.product_term,accru.cost_center 
		, accru.accru_dim_txt
		, 'accrued-ac' as source_filename , 'natrn-accrueac' as  group_nm  
		, 'Accruedac_current' as subgroup_nm , 'V12-รายการคืนเบี้ยสินไหมปฏิเสธค้างจ่าย ณ สิ้นเดือน'  as subgroup_desc 
 		, vn.variable_cd
		, vn.variable_name as variable_nm 
		, natrn.detail as nadet_detail ,accru.org_branch_cd 
		, ''::varchar as org_submit_no , ''::varchar as submit_no , ''::varchar as section_order_no ,0 as is_section_order_no
		,natrn.for_branch_cd
		,vn.event_type  
		,natrn.claim_gmm_flg,natrn.claim_gmm_desc 		
		,pol.policy_type,pol.effective_dt ,pol.issued_dt  
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup 
		,vn.is_duplicate_variable ,vn.duplicate_fr_variable_nm 
		from dds.vw_ifrs_accrual_chg accru  
		inner join stag_a.stga_ifrs_nac_txn_step04 natrn
		on ( natrn.branch_cd = accru.branch_cd
		and natrn.doc_dt = accru.doc_dt
		and natrn.doc_type = accru.doc_type
		and natrn.doc_no = accru.doc_no 
		and natrn.accode = accru.accode
		and natrn.dc_flg = accru.dc_flg 
		and natrn.natrn_dim_txt =  accru.accru_dim_txt)
		left outer join stag_a.stga_sum_ifrs_claim_trans claim 
		on ( trim(accru.ref_no) = claim.section_order_no
			and accru.policy_no = claim.policy_no)
		 left join  stag_s.stg_tb_core_planspec pp 
		 on ( accru.plan_code = pp.plan_cd)
		 inner join stag_s.ifrs_common_variable_nm vn
		 on ( vn.event_type = 'Claim'
		 and vn.variable_cd = 'V12'
		 and vn.claim_gmm_flg  = natrn.claim_gmm_flg )  
		 left outer join dds.tl_acc_policy_chg pol  
		 on (  accru.policy_no = pol.policy_no 
		 and accru.plan_code = pol.plan_cd
		 and accru.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm )  
		 where accru.doc_dt between  v_control_start_dt and v_control_end_dt ; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP18 V11 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
 	raise notice 'STEP18 % : % row(s) - V11 รายการคืนเบี้ยสินไหมปฏิเสธค้างจ่าย ณ สิ้นเดือน',clock_timestamp()::varchar(19),v_affected_rows::varchar;
 
   -- 17Oct2024 Narumol W.: [ITRQ#67094481] เพิ่ม source ของบัญชีสัญญาพิเศษเพิ่มเติมค้างจ่าย-รักษาพยาบาล(รพ.) V12
begin   
	 
		insert into stag_a.stga_ifrs_nac_txn_step05_claim 
		select  gl.ref1_header as ref_1 ,adth.adth_fax_claim_rk as adth_rk  ,null::bigint as adth_pay_rk , null::bigint natrn_x_dim_rk  
			,adth.branch_cd 
			,gl.doc_type as sap_doc_type ,left(gl.ref1_header ,6) as ref1_header 
			--,gl.posting_dt  as post_dt   
			,gl.report_dt as post_dt      
			,null::date ,null::date ,null::date 
			,coalesce(adth.doc_dt,gl.doc_dt ) as doc_dt ,adth.doc_type,adth.doc_no
			--,gl.accode  as accode
			,case when gl.accode = '1993020060' then '2012030090' else gl.accode end as  accode
			,adth.dc_flg,null::varchar as actype 
			--,adth.posting_claim_amt	
			,case when adth.accode = '1993020060' then adth.posting_claim_amt*-1  else adth.posting_claim_amt end as  posting_claim_amt
			,adth.claim_amt
			--,adth.posting_claim_amt as posting_claim_pay_amt
			,case when adth.accode = '1993020060' then adth.posting_claim_amt*-1  else adth.posting_claim_amt end as  posting_claim_pay_amt
			, adth.claim_amt  as claim_pay_amt
			,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
			,adth.policy_no,adth.plan_cd::varchar(10), adth.rider_cd,null::varchar as pay_status_cd ,null::date pay_dt ,null::varchar pay_by_channel
			,0::numeric as sum_natrn_amt  
			,gl.posting_amt ,gl.posting_amt as posting_proxy_amt
			,adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
			,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
			,adth.adthtran_dim_txt as adth_dim_txt
			,adth.source_filename 
			--, 'adth_payment_api' as source_filename 
			, 'outstanding_balance-adth_payment_api' as  group_nm  
			, 'Accruedac_current' as subgroup_nm , 'V12-บัญชีสัญญาพิเศษเพิ่มเติมค้างจ่าย-รักษาพยาบาล(รพ.)'  as subgroup_desc  
		 	, vn.variable_cd 
			, coalesce ( vn.variable_name,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
			, null::varchar as nadet_detail 
			, adth.org_branch_cd
			, null::varchar as org_submit_no
			, null::varchar as submit_no
			, adth.section_order_no
			, 1 as is_section_order_no
			, adth.branch_cd as for_branch_cd
			, coa.event_type  
			, coa.claim_gmm_flg,coa.claim_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup  
			, 0::int is_duplicate_variable
			, null::varchar duplicate_fr_variable_nm
		from dds.sapgl_gli006_outstanding_balance gl 
		left outer join dds.ifrs_adth_payment_api_chg adth 
		on ( gl.ref1_header  = adth.ref_header  )
		inner join stag_s.ifrs_common_coa coa 
	    on ( gl.accode = coa.accode 
	    and coa.event_type  = 'Claim')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( adth.policy_no  = pol.policy_no
	    and adth.plan_cd  = pol.plan_cd 
	    and gl.posting_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)  
	    inner join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Claim'  
	    and vn.variable_cd in ('V12')
	    and coa.claim_gmm_flg  = vn.claim_gmm_flg )  
		where gl.doc_type = 'KI'
		and gl.report_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 		raise notice 'STEP18.1 % : % row(s) - V12 บัญชีสัญญาพิเศษเพิ่มเติมค้างจ่าย-รักษาพยาบาล(รพ.)',clock_timestamp()::varchar(19),v_affected_rows::varchar;

		insert into stag_a.stga_ifrs_nac_txn_step05_claim
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt ,claim_dt
		, doc_dt ,accode , dc_flg , claim_amt  
		, posting_claim_amt  , policy_no 
		, plan_cd ,rider_cd
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,claim_gmm_flg ,claim_gmm_desc
		, policy_type, effective_dt,issued_dt
		,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable   )		 
		select gl.ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , ob.doc_type as sap_doc_type , reference_header 
		--, gl.posting_date_dt  as post_dt 
		, ob.report_dt as post_dt 
		, ob.doc_dt as claim_dt, ob.doc_dt ,account_no as sap_acc_cd ,dc_flg 
		, gl.local_sap_amt ,gl.posting_sap_amt  
		, gl.policy_no 
		, coalesce(gl.plan_cd,pol.plan_cd)::varchar(10) as plan_cd
		, null as rider_cd
		, gl.posting_sap_amt ,gl.posting_sap_amt  as posting_proxy_amt
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		,'SAP' as source_filename 			 
		, 'outstanding_balance-sap' as  group_nm  
		, 'Accruedac_current' as subgroup_nm , 'V12-บัญชีสัญญาพิเศษเพิ่มเติมค้างจ่าย-รักษาพยาบาล(รพ.)'  as subgroup_desc 
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.claim_gmm_flg , coa.claim_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		from stag_a.stga_ifrs_nac_txn_step01 gl
		inner join dds.sapgl_gli006_outstanding_balance ob 
		on ( gl.doc_no = ob.doc_no 
		and gl.doc_type = ob.doc_type
		and gl.posting_date_dt  = ob.posting_dt 
		and gl.account_no  = ob.accode ) 
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Claim')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd  = pol.plan_cd  
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)  
	    left join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Claim'  
	    and vn.variable_cd in ('V12')
	    and coa.claim_gmm_flg  = vn.claim_gmm_flg ) 
		where coa.event_type  = 'Claim' 
		and ob.doc_type <> 'KI' 
		and vn.is_duplicate_variable <> 1 
		and ob.report_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
 		raise notice 'STEP18.2 % : % row(s) - V12 บัญชีสัญญาพิเศษเพิ่มเติมค้างจ่าย-รักษาพยาบาล(รพ.)',clock_timestamp()::varchar(19),v_affected_rows::varchar;

	  	EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP18 V11 : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 
  
 
-- =================================== MANUAL from GL SAP =========================== -- 
	BEGIN	
		-- Oil 25Nov2022 : เพิ่ม policy_no , plan_cd 
		------------- Manual  
		insert into stag_a.stga_ifrs_nac_txn_step05_claim
		( ref_1 , branch_cd ,sap_doc_type,reference_header,post_dt ,claim_dt
		, doc_dt ,accode , dc_flg , claim_amt 
		--, posting_claim_pay_amt 
		, posting_claim_amt  , policy_no 
		, plan_cd
		, posting_sap_amt ,posting_proxy_amt  
		, selling_partner,distribution_mode,product_group
		, product_sub_group,rider_group,product_term,cost_center,nac_dim_txt 
		, source_filename ,group_nm ,subgroup_nm ,subgroup_desc
		, variable_cd ,variable_nm,nadet_detail , event_type ,claim_gmm_flg ,claim_gmm_desc
		, policy_type, effective_dt,issued_dt
		, ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode ,ifrs17_portid ,ifrs17_portgroup,is_duplicate_variable
		, rider_cd)			
		select ref1_header as ref_1 , right(processing_branch_cd,3) as branch_cd , doc_type as sap_doc_type , reference_header , posting_date_dt  as post_dt 
		, doc_dt as claim_dt, doc_dt ,account_no as sap_acc_cd ,dc_flg 
		, local_sap_amt ,posting_sap_amt  
		, gl.policy_no 
		, coalesce(gl.plan_cd,pol.plan_cd)::varchar(10) as plan_cd
		, gl.posting_sap_amt ,gl.posting_sap_amt  as posting_proxy_amt
		, gl.selling_partner,gl.distribution_mode,gl.product_group
		, gl.product_sub_group,gl.rider_group,gl.term_cd ,gl.cost_center,sap_dim_txt 
		,'SAP' as source_filename 			
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, vn.variable_cd, vn.variable_name as variable_nm  
		, gl.description_txt  
		, coa.event_type , coa.claim_gmm_flg , coa.claim_gmm_desc 
		, pol.policy_type ,pol.effective_dt ,pol.issued_dt 
		, pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		, pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable 
		, gl.rider_cd -- 29Oct2024 Nuttadet O.: [ITRQ#67105145] เพิ่มเงื่อนไข Variable Mapping สำหรับ source SAP manual โดยจับเงื่อนไข rider จาก ref key 3 (add col. rider_cd)
		from stag_a.stga_ifrs_nac_txn_step01 gl
	    inner join stag_s.ifrs_common_coa coa 
	    on ( gl.account_no = coa.accode 
	    and coa.event_type  = 'Claim')
	    left outer join dds.tl_acc_policy_chg pol  
	    on ( gl.policy_no  = pol.policy_no
	    and gl.plan_cd  = pol.plan_cd -- Oil 25jan2023 : add plan_cd
	    and gl.posting_date_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
	    left join  stag_s.stg_tb_core_planspec pp 
	    on ( pol.plan_cd = pp.plan_cd)  
	    left join stag_s.ifrs_common_variable_nm vn
	    on ( vn.event_type = 'Claim'  
	    and vn.variable_cd in ('V1','V3')
	    and coa.claim_gmm_flg  = vn.claim_gmm_flg ) 
		where coa.event_type  = 'Claim' 
		and doc_type not in ( 'SI','SB','KI')
		and accode not in ('2012030080')
		and vn.is_duplicate_variable <> 1
		and posting_date_dt  between v_control_start_dt and v_control_end_dt;
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP19 VX Manual PROXY : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STE19 % : % row(s) - VX - Manual PROXY ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 

	-- =================================== KI - Fax Claim : Log.210 =========================== -- 
	BEGIN	
			insert into stag_a.stga_ifrs_nac_txn_step05_claim
			select  gl.ref1_header as ref_1 ,adth.adth_fax_claim_rk as adth_rk  ,null::bigint as adth_pay_rk , null::bigint natrn_x_dim_rk  
			,adth.branch_cd 
			,gl.doc_type as sap_doc_type ,left(gl.ref1_header ,6) as ref1_header ,gl.posting_date_dt as post_dt 
			--,claim.receive_dt as claim_dt  ,claim.accident_dt as accident_dt  ,claim.claimok_dt  as claim_ok_dt 
			,null::date ,null::date ,null::date 
			,coalesce(adth.doc_dt,gl.doc_dt ) as doc_dt ,adth.doc_type,adth.doc_no,gl.account_no as accode,adth.dc_flg,null::varchar as actype 
			,adth.posting_claim_amt	,adth.claim_amt
			,adth.posting_claim_amt as posting_claim_pay_amt, adth.claim_amt  as claim_pay_amt
			,null::varchar system_type,null::varchar transaction_type,adth.premium_flg as premium_type
			,adth.policy_no,adth.plan_cd::varchar(10), adth.rider_cd,null::varchar as pay_status_cd ,null::date pay_dt ,null::varchar pay_by_channel
			,0::numeric as sum_natrn_amt  
			,gl.posting_sap_amt,gl.posting_sap_amt as posting_proxy_amt
			,adth.sales_id,adth.sales_struct_n,adth.selling_partner,adth.distribution_mode,adth.product_group
			,adth.product_sub_group,adth.rider_group,adth.product_term,adth.cost_center
			,adth.adthtran_dim_txt as adth_dim_txt
			,adth.source_filename , 'sap-fax claim' as  group_nm  , 'sap-fax claim' as subgroup_nm ,  'Fax Claim' as subgroup_desc 
		 	,'DUM_NT'::varchar  as variable_cd     
			, coalesce ( coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
			, null::varchar as nadet_detail 
			, adth.org_branch_cd
			, null::varchar as org_submit_no
			, null::varchar as submit_no
			, adth.section_order_no
			, 1 as is_section_order_no
			, adth.branch_cd as for_branch_cd
			, coa.event_type  
			, coa.claim_gmm_flg,coa.claim_gmm_desc 
			,pol.policy_type ,pol.effective_dt ,pol.issued_dt
			,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
			,pp.ifrs17_portid ,pp.ifrs17_portgroup  
			, 0::int is_duplicate_variable
			, null::varchar duplicate_fr_variable_nm
			from stag_a.stga_ifrs_nac_txn_step01 gl  
			left join  dds.ifrs_adth_payment_api_chg adth  
			on ( gl.ref1_header = adth.ref_header 
			and gl.account_no  = adth.accode 
			and gl.sap_dim_txt = adth.adthtran_dim_txt
			and adth.org_branch_cd in ( 'CCL','GCL' )
			)
			/*left outer join  
			(select section_order_no , policy_no , rider_type 
				, receive_dt as receive_dt 
				,max(accident_dt) as accident_dt 
				,max(claimok_dt)  as claimok_dt 
				,max(pay_dt) as pay_dt
				from  stag_a.stga_ifrs_claim_trans 
				where trim(section_order_no) <> ''
				group by section_order_no , policy_no ,rider_type, receive_dt )	 claim
			on (adth.section_order_no = claim.section_order_no 
			and adth.policy_no = claim.policy_no
			and adth.rider_cd = claim.rider_type)*/
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
			where coa.event_type = 'Claim'    
			 and gl.doc_type  = 'KI'  
			 and adth.policy_no is null 
			 and gl.posting_date_dt between v_control_start_dt and v_control_end_dt;
			 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP19  :  DUMMY KI :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 

 	raise notice 'STEP19 % : % row(s) - DUMMY KI ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
	
 
 	-- ===================================== V7 ==============================================================--
 
 	BEGIN	
 
		-- Reconcile 
		drop table if exists stag_a.stga_ifrs_nac_txn_missing_step05_claim;
		create table stag_a.stga_ifrs_nac_txn_missing_step05_claim tablespace tbs_stag_a as 
		select step4.* --  step4.ref_1 , step4.branch_cd ,step4.doc_dt , step4.doc_type,step4.doc_no, step4.accode ,sum(posting_natrn_amt) as posting_natrn_amt
		from  stag_a.stga_ifrs_nac_txn_step04 step4
		left outer join  stag_a.stga_ifrs_nac_txn_step05_claim step5
		--on ( step4.ref_1 = step5.ref_1
		on ( step4.branch_cd = step5.branch_cd
		and step4.doc_dt = step5.doc_dt 
		and step4.doc_no = step5.doc_no ) 
		where step5.ref_1 is null 
		-- 08Sep2022 Narumol W.: Claim Log 206 Remove condition unpaid approve claim 
		--and step4.accode not in ( '2012030080') 
		--and step4.dc_flg = 'C' --Log.90 
		and step4.doc_dt between  v_control_start_dt and v_control_end_dt  
		and step4.accode in ( select accode from stag_s.ifrs_common_coa 
									where event_type = 'Claim'
									and claim_gmm_flg <> '5' ); -- Log.211
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP20 Reconcile : p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 

 	raise notice 'STEP20 % : % row(s) - Reconcile ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 
 	begin
		------------- DUMMY   
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  	
		select  ref_1,adth_rk,adth_pay_rk,natrn_x_dim_rk,branch_cd,sap_doc_type,reference_header,post_dt
		,claim_dt,accident_dt,claim_ok_dt,doc_dt,doc_type,doc_no,accode,dc_flg,actype
		,posting_amt,nac_amt,posting_amt,nac_amt,system_type,transaction_type,premium_type
		,policy_no,plan_cd,rider_cd,pay_status_cd,pay_dt,pay_by_channel,sum_natrn_amt
		,posting_sap_amt,posting_proxy_amt,sales_id,sales_struct_n,selling_partner
		,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
		,source_filename,group_nm,subgroup_nm,subgroup_desc,variable_cd,variable_nm,nadet_detail
		,org_branch_cd,org_submit_no,submit_no,section_order_no,is_section_order_no,for_branch_cd
		,event_type,claim_gmm_flg,claim_gmm_desc,policy_type,effective_dt,issued_dt
		,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid
		,ifrs17_portgroup,is_duplicate_variable 
		from ( 
		select  natrn.ref_1 as ref_1 
		, null::bigint as adth_rk 
		, null::bigint as adth_pay_rk  
		, natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, null::date as claim_dt
		, null::date as accident_dt
		, null::date as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.natrn_amt as nac_amt
		,natrn.posting_natrn_amt as posting_amt
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,coalesce(pol.plan_cd,natrn.plan_cd ) plan_cd
		,null::varchar as rider_cd  
		,null::varchar as pay_status_cd,null::date as pay_dt ,null::varchar as pay_by_channel 
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
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
		,natrn.claim_gmm_flg ,natrn.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
								order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd) as _rk 
		from  stag_a.stga_ifrs_nac_txn_missing_step05_claim natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd) 
		left outer join dds.tl_acc_policy_chg pol    
		on ( natrn.nadet_policyno  = pol.policy_no
		and  natrn.plan_cd= pol.plan_cd 
		and natrn.doc_dt between pol.valid_fr_dttm and pol.valid_to_dttm ) 
		left outer join stag_s.ifrs_common_coa coa 
		on ( natrn.accode = coa.accode  )  
		inner join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Claim'  
		and vn.variable_cd  in ( 'V1','V3')
		and natrn.claim_gmm_flg  = vn.claim_gmm_flg ) 
		where natrn.event_type  = 'Claim' 
		--and natrn.branch_cd = '000'
		--and natrn.post_dt  between v_control_start_dt and v_control_end_dt
		)  as aa 
		where _rk =1 ; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	
 		raise notice 'STEP21 % : % row(s) - Dummy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

/*	
		insert into stag_a.stga_ifrs_nac_txn_step05_claim  	 
		select  ref_1,adth_rk,adth_pay_rk,natrn_x_dim_rk,branch_cd,sap_doc_type,reference_header,post_dt
		,claim_dt,accident_dt,claim_ok_dt,doc_dt,doc_type,doc_no,accode,dc_flg,actype
		,posting_amt,nac_amt,posting_amt,nac_amt,system_type,transaction_type,premium_type
		,policy_no,plan_cd,rider_cd,pay_status_cd,pay_dt,pay_by_channel,sum_natrn_amt
		,posting_sap_amt,posting_proxy_amt,sales_id,sales_struct_n,selling_partner
		,distribution_mode,product_group,product_sub_group,rider_group,product_term,cost_center,nac_dim_txt
		,source_filename,group_nm,subgroup_nm,subgroup_desc,variable_cd,variable_nm,nadet_detail
		,org_branch_cd,org_submit_no,submit_no,section_order_no,is_section_order_no,for_branch_cd
		,event_type,claim_gmm_flg,claim_gmm_desc,policy_type,effective_dt,issued_dt
		,ifrs17_channelcode,ifrs17_partnercode,ifrs17_portfoliocode,ifrs17_portid
		,ifrs17_portgroup,is_duplicate_variable 
		from ( 
		select  natrn.ref_1 as ref_1 
		, null::bigint as adth_rk 
		, null::bigint as adth_pay_rk  
		, natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, null::date as claim_dt
		, null::date as accident_dt
		, null::date as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.natrn_amt as nac_amt
		,natrn.posting_natrn_amt as posting_amt
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no,natrn.plan_cd,null::varchar as rider_cd  
		,null::varchar as pay_status_cd,null::date as pay_dt ,null::varchar as pay_by_channel 
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename  
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, coalesce (vn.variable_cd,coa.dummy_variable_nm) as variable_cd  
		, coalesce (vn.variable_name,d.dummy_var_nm_branch,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.claim_gmm_flg ,natrn.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
								order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd) as _rk 
		from  stag_a.stga_ifrs_nac_txn_missing_step05_claim natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd)
		left outer join stag_a.stga_tl_acc_policy pol  
		on ( natrn.nadet_policyno  = pol.policy_no 
		and  natrn.plan_cd= pol.plan_cd )
		left outer join stag_s.ifrs_common_coa coa 
		on ( natrn.accode = coa.accode 
		and natrn.claim_gmm_flg = coa.claim_gmm_flg)  
		left join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Claim' 
		and vn.variable_cd  in ( 'V2','V3')
		and natrn.claim_gmm_flg  = vn.claim_gmm_flg ) 
		left outer join dds.ifrs_common_dummy_var_nm d 
		on ( coa.event_type = d.event_type  
		and coa.variable_for_policy_missing = d.dummy_var_nm_ho )
		where natrn.event_type  = 'Claim' 
		and natrn.branch_cd <> '000' 
		and natrn.plan_cd is not null 
		
		union all 
		select  natrn.ref_1 as ref_1 
		, null::bigint as adth_rk 
		, null::bigint as adth_pay_rk  
		, natrn.natrn_x_dim_rk  
		,natrn.branch_cd,natrn.sap_doc_type,natrn.reference_header,natrn.post_dt
		, null::date as claim_dt
		, null::date as accident_dt
		, null::date as claim_ok_dt
		,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,null::varchar as actype 
		,natrn.natrn_amt as nac_amt
		,natrn.posting_natrn_amt as posting_amt
		,null::varchar as system_type,null::varchar as transaction_type,null::varchar as premium_type
		,natrn.nadet_policyno as policy_no
		,pol.plan_cd,null::varchar as rider_cd  
		,null::varchar as pay_status_cd,null::date as pay_dt ,null::varchar as pay_by_channel 
		,posting_natrn_amt as sum_natrn_amt ,posting_sap_amt,posting_proxy_amt 
		,natrn.sales_id,natrn.sales_struct_n
		,natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center
		,concat(natrn.selling_partner,natrn.distribution_mode,natrn.product_group,natrn.product_sub_group,natrn.rider_group,natrn.product_term,natrn.cost_center) as nac_dim_txt  
		,natrn.filename as source_filename  
		, 'Manual'::varchar(100) as  group_nm , 'accounting transaction'::varchar(100) as subgroup_nm , 'ข้อมูลบันทึกบัญชี' ::varchar(100) as subgroup_desc  
		, coalesce (vn.variable_cd,coa.dummy_variable_nm) as variable_cd  
		, coalesce (vn.variable_name,d.dummy_var_nm_branch,coa.variable_for_policy_missing ,'DUM_NT') as variable_nm  
		, natrn.detail as nadet_detail 
		, natrn.org_branch_cd, null::varchar as org_submit_no
		, null::varchar as submit_no
		, null::varchar as section_order_no
		, 0 as is_section_order_no
		,natrn.for_branch_cd
		,natrn.event_type 
		,natrn.claim_gmm_flg ,natrn.claim_gmm_desc 
		,pol.policy_type ,pol.effective_dt ,pol.issued_dt
		,pp.ifrs17_channelcode ,pp.ifrs17_partnercode ,pp.ifrs17_portfoliocode 
		,pp.ifrs17_portid ,pp.ifrs17_portgroup ,vn.is_duplicate_variable  
		, row_number() over  ( partition by natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg 
								order by  natrn.natrn_x_dim_rk ,natrn.doc_dt,natrn.doc_type,natrn.doc_no,natrn.accode,natrn.dc_flg,vn.variable_cd) as _rk 
		from  stag_a.stga_ifrs_nac_txn_missing_step05_claim natrn 
		left join  stag_s.ifrs_common_accode acc 
		on ( natrn.accode = acc.account_cd  )  
		left join  stag_s.stg_tb_core_planspec pp 
		on ( natrn.plan_cd = pp.plan_cd)
		left outer join stag_a.stga_tl_acc_policy pol  
		on ( natrn.nadet_policyno  = pol.policy_no)
		--and  natrn.plan_cd= pol.plan_cd )
		left outer join stag_s.ifrs_common_coa coa 
		on ( natrn.accode = coa.accode 
		and natrn.claim_gmm_flg = coa.claim_gmm_flg)  
		left join stag_s.ifrs_common_variable_nm vn 
		on ( vn.event_type = 'Claim' 
		and vn.variable_cd  in ( 'V2','V3')
		and natrn.claim_gmm_flg  = vn.claim_gmm_flg ) 
		left outer join dds.ifrs_common_dummy_var_nm d 
		on ( coa.event_type = d.event_type  
		and coa.variable_for_policy_missing = d.dummy_var_nm_ho )
		where natrn.event_type  = 'Claim' 
		and natrn.branch_cd <> '000' 
		and natrn.plan_cd is null 
		
		)  as aa 
		where _rk =1 ;  
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
	*/
 		raise notice 'STEP19 % : % row(s) - Dummy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP18 VX Dummy: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP18 % : % row(s) - VX - Dummy ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   


	--=============== COHORT TAG GROUPEB ====================--
 	begin  
	 	
	 	-- Log.66
	 	-- 10Feb2023 Narumol W. : update icgid for PAA
		/*update stag_a.stga_ifrs_nac_txn_step05_claim  	 
		set variable_cd  = 'GI1_PF_2020_M907_M907_GB_799'
		where plan_cd   in (  'M907','GEB1','GEB2','PL34' ) ;
	 	*/ 
		update stag_a.stga_ifrs_nac_txn_step05_claim 
		set variable_cd  = i.icg_id 
		from stag_s.stg_common_icgid i 
		where stag_a.stga_ifrs_nac_txn_step05_claim.plan_cd =i.plan_cd 
		and  stag_a.stga_ifrs_nac_txn_step05_claim.plan_cd in (  'M907','GEB1','GEB2','PL34' ) ;
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP19 % : % row(s) - Update icg_id ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   
	
		update stag_a.stga_ifrs_nac_txn_step05_claim 
		set variable_cd  = i.icg_id 
		from stag_s.stg_common_icgid i 
		where stag_a.stga_ifrs_nac_txn_step05_claim.variable_cd like 'V%'
		and stag_a.stga_ifrs_nac_txn_step05_claim.product_group='17' 
		and stag_a.stga_ifrs_nac_txn_step05_claim.product_sub_group='704'
		and i.plan_cd ='GEB1'; 
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP20 % : % row(s) - Update icg_id',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   
	 
		-- 02Oct2023 Narumol W. : Patch policy_no and plan_cd
		update stag_a.stga_ifrs_nac_txn_step05_claim p
		set  policy_no = pol.policy_no 
		,plan_cd = pol.plan_cd
		from dds.tl_acc_policy_chg pol 
		where ( p.policy_no = pol.plan_cd || pol.policy_no 
		and p.doc_dt between pol.valid_fr_dttm  and pol.valid_to_dttm ) 
		and length(p.policy_no) = 12 
		and p.sap_doc_type in ('SI','SB');
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		raise notice 'STEP18 % : % row(s) - Patch policy_no and plan_cd',clock_timestamp()::varchar(19),v_affected_rows::varchar;	   
	
		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP30 COHORT TAG GROUPEB: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;  
	raise notice 'STEP30 % : % row(s) - COHORT TAG GROUPEB ',clock_timestamp()::varchar(19),v_affected_rows::varchar;	 
  	--============================================================ 

	-- 16Oct2025 Nuttadet O. : [ITRQ#68104275] นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN
	begin
		
		if date_part('month',v_control_start_dt) = 1  then 
		
			insert into stag_a.stga_ifrs_nac_txn_step05_claim  
			select ref_1 ,adth_rk ,adth_pay_rk ,natrn_x_dim_rk ,branch_cd
			,sap_doc_type ,reference_header 
			,v_control_end_dt as post_dt
			,claim_dt ,accident_dt
			,claim_ok_dt ,doc_dt ,doc_type ,doc_no ,accode ,dc_flg ,actype
			,posting_claim_amt * -1 as posting_claim_amt
			,claim_amt ,posting_claim_pay_amt ,claim_pay_amt
			,system_type ,transaction_type ,premium_type ,policy_no
			,plan_cd ,rider_cd ,pay_status_cd ,pay_dt ,pay_by_channel
			,sum_natrn_amt ,posting_sap_amt ,posting_proxy_amt
			,sales_id ,sales_struct_n ,selling_partner
			,distribution_mode ,product_group ,product_sub_group
			,rider_group ,product_term ,cost_center ,nac_dim_txt ,source_filename
			,group_nm 
			,'Accrued_bop' as subgroup_nm ,subgroup_desc ,variable_cd ,variable_nm ,nadet_detail 
			,org_branch_cd ,org_submit_no ,submit_no ,section_order_no
			,is_section_order_no ,for_branch_cd ,event_type ,claim_gmm_flg
			,claim_gmm_desc ,policy_type ,effective_dt ,issued_dt
			,ifrs17_channelcode ,ifrs17_partnercode ,ifrs17_portfoliocode
			,ifrs17_portid ,ifrs17_portgroup ,is_duplicate_variable ,duplicate_fr_variable_nm 
			,fee_cd
			from dds.ifrs_variable_claim_ytd a
			where a.control_dt =  v_control_end_dt - interval '1 month'
			and variable_nm in ('ACTUAL_CLAIM_U_PDEX_CS');		

		GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
		end if;
	
		EXCEPTION 
		WHEN OTHERS THEN 
			out_err_cd := 1;
			out_err_msg := SQLERRM;
			INSERT INTO dds.tb_fn_log
			VALUES ( 'dds.ifrs_variable_txn05_claim' 				
				,'ERROR STEP31  :  VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด :'||p_xtr_start_dt::VARCHAR(15) 	
			,SQLSTATE,SQLERRM,clock_timestamp());		
			 RETURN out_err_cd ;  
			
	END;	 

 	raise notice 'STEP31 % : % row(s) - VX นำข้อมูล สิ้นปีมาตั้งยอด เป็น Bop ณ เดือน JAN เพื่อหักล้างยอด ',clock_timestamp()::varchar(19),v_affected_rows::varchar;

 	--=============== Insert into YTD ==================--
  	select dds.fn_ifrs_variable_claim(p_xtr_start_dt) into out_err_cd  ;

	-- Complete
	INSERT INTO dds.tb_fn_log
	VALUES ( 'dds.ifrs_variable_txn05_claim','COMPLETE: p_xtr_start_dt :'||p_xtr_start_dt::VARCHAR(15),0,'',clock_timestamp());	
	
   	return i;
END

$function$
;
