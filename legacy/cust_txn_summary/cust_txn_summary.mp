@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|9|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|26|rel_count|37|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|2|0|2|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|PROCESS_DATE|$[(date("YYYYMMDD"))now1()]|3|2|$|@{0|}}
{30001002|XXparameter|MIN_AMOUNT|0|3|2|$|@{0|}}
}}@0|@0|0|0|0|0|}
{2010503005|XXGfvertex|3|0|3|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Input_File.mdc|3|2|$|@{0|}}
{30001002|XXparameter|Layout|file:$AI_SERIAL/txn.dat|3|2|$|@{0|}}
{30001002|XXparameter|read_metadata|record
  string("\|") txn_id;
  decimal("\|") cust_id;
  date("YYYY-MM-DD")("\|") txn_date;
  decimal("\|", 2) amount;
  string("\|") currency;
  string("\\n") channel = NULL("");
end;|3|2|$|@{0|}}
}}@0|Input_File|0|0|0|0|}}
{2010203004|XXGoport|5|0|5|0|{@{}@0|0|0|0|read|0.0|@@@0|0|}}
{2010501005|XXGpvertex|7|0|7|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|$|@{0|}}
{30001002|XXparameter|in_metadata||3|2|$|@{0|}}
{30001002|XXparameter|out0_metadata|record
  string("\|") txn_id;
  decimal("\|") cust_id;
  date("YYYY-MM-DD")("\|") txn_date;
  decimal("\|", 2) amount;
  string("\|") currency;
  string("\\n") channel = NULL("");
end;|3|2|$|@{0|}}
{30001002|XXparameter|transform0|out :: reformat(in) =
begin
  out.txn_id :: string_lrtrim(in.txn_id);
  out.cust_id :: in.cust_id;
  out.txn_date :: in.txn_date;
  out.amount :: in.amount;
  out.currency :: string_upcase(string_lrtrim(in.currency));
  out.channel :: first_defined(in.channel, "UNKNOWN");
end;|3|2|$|@{0|}}
}}@0|Clean_Txn|0|0|0|0|}}
{2010202004|XXGiport|9|0|9|0|{@{}@0|0|0|0|in|0.0|@@@0|0|}}
{2010203004|XXGoport|11|0|11|0|{@{}@0|0|0|0|out0|0.0|@@@0|0|}}
{2010501005|XXGpvertex|13|0|13|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Filter_by_Expression.mpc|3|2|$|@{0|}}
{30001002|XXparameter|select_expr|amount > 0|3|2|$|@{0|}}
{30001002|XXparameter|out_metadata|record
  string("\|") txn_id;
  decimal("\|") cust_id;
  date("YYYY-MM-DD")("\|") txn_date;
  decimal("\|", 2) amount;
  string("\|") currency;
  string("\\n") channel = NULL("");
end;|3|2|$|@{0|}}
}}@0|Valid_Amount|0|0|0|0|}}
{2010202004|XXGiport|15|0|15|0|{@{}@0|0|0|0|in|0.0|@@@0|0|}}
{2010203004|XXGoport|17|0|17|0|{@{}@0|0|0|0|out|0.0|@@@0|0|}}
{2010203004|XXGoport|19|0|19|0|{@{}@0|0|0|0|deselect|0.0|@@@0|0|}}
{2010501005|XXGpvertex|21|0|21|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Sort/Sort.mpc|3|2|$|@{0|}}
{30001002|XXparameter|key|\{cust_id\}|3|2|$|@{0|}}
{30001002|XXparameter|out_metadata|record
  string("\|") txn_id;
  decimal("\|") cust_id;
  date("YYYY-MM-DD")("\|") txn_date;
  decimal("\|", 2) amount;
  string("\|") currency;
  string("\\n") channel = NULL("");
end;|3|2|$|@{0|}}
}}@0|Sort_Cust|0|0|0|0|}}
{2010202004|XXGiport|23|0|23|0|{@{}@0|0|0|0|in|0.0|@@@0|0|}}
{2010203004|XXGoport|25|0|25|0|{@{}@0|0|0|0|out|0.0|@@@0|0|}}
{2010501005|XXGpvertex|27|0|27|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Rollup.mpc|3|2|$|@{0|}}
{30001002|XXparameter|key|\{cust_id\}|3|2|$|@{0|}}
{30001002|XXparameter|out_metadata|record
  decimal("\|") cust_id;
  decimal("\|") txn_count;
  decimal("\|", 2) total_amount;
  date("YYYY-MM-DD")("\\n") last_txn_date;
end;|3|2|$|@{0|}}
{30001002|XXparameter|transform|out :: rollup(in) =
begin
  out.cust_id :: in.cust_id;
  out.txn_count :: count(in.txn_id);
  out.total_amount :: sum(in.amount);
  out.last_txn_date :: max(in.txn_date);
end;|3|2|$|@{0|}}
}}@0|Rollup_Cust|0|0|0|0|}}
{2010202004|XXGiport|29|0|29|0|{@{}@0|0|0|0|in|0.0|@@@0|0|}}
{2010203004|XXGoport|31|0|31|0|{@{}@0|0|0|0|out|0.0|@@@0|0|}}
{2010503005|XXGfvertex|33|0|33|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|$|@{0|}}
{30001002|XXparameter|Layout|file:$AI_SERIAL/cust_summary.dat|3|2|$|@{0|}}
{30001002|XXparameter|write_metadata|record
  decimal("\|") cust_id;
  decimal("\|") txn_count;
  decimal("\|", 2) total_amount;
  date("YYYY-MM-DD")("\\n") last_txn_date;
end;|3|2|$|@{0|}}
}}@0|Output_Summary|0|0|0|0|}}
{2010202004|XXGiport|35|0|35|0|{@{}@0|0|0|0|write|0.0|@@@0|0|}}
{2010503005|XXGfvertex|37|0|37|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|$|@{0|}}
{30001002|XXparameter|Layout|file:$AI_SERIAL/txn_rejects.dat|3|2|$|@{0|}}
{30001002|XXparameter|write_metadata|record
  string("\|") txn_id;
  decimal("\|") cust_id;
  date("YYYY-MM-DD")("\|") txn_date;
  decimal("\|", 2) amount;
  string("\|") currency;
  string("\\n") channel = NULL("");
end;|3|2|$|@{0|}}
}}@0|Output_Rejects|0|0|0|0|}}
{2010202004|XXGiport|39|0|39|0|{@{}@0|0|0|0|write|0.0|@@@0|0|}}
{2010210004|XXGflow|41|0|41|0|{@{}@0|.5|.5|{0|}0|0|}}
{2010210004|XXGflow|45|0|45|0|{@{}@0|.5|.5|{0|}0|0|}}
{2010210004|XXGflow|49|0|49|0|{@{}@0|.5|.5|{0|}0|0|}}
{2010210004|XXGflow|53|0|53|0|{@{}@0|.5|.5|{0|}0|0|}}
{2010210004|XXGflow|57|0|57|0|{@{}@0|.5|.5|{0|}0|0|}}
{2010210004|XXGflow|61|0|61|0|{@{}@0|.5|.5|{0|}0|0|}}
{2010601001|XXGgraph_vertex_vertex|4|0|4|0|{Input_File|}2|3|}
{2010602001|XXGvertex_oport_oport|6|0|6|0|{0|read|}3|5|}
{2010601001|XXGgraph_vertex_vertex|8|0|8|0|{Clean_Txn|}2|7|}
{2010602001|XXGvertex_iport_iport|10|0|10|0|{0|in|}7|9|}
{2010602001|XXGvertex_oport_oport|12|0|12|0|{0|out0|}7|11|}
{2010601001|XXGgraph_vertex_vertex|14|0|14|0|{Valid_Amount|}2|13|}
{2010602001|XXGvertex_iport_iport|16|0|16|0|{0|in|}13|15|}
{2010602001|XXGvertex_oport_oport|18|0|18|0|{0|out|}13|17|}
{2010602001|XXGvertex_oport_oport|20|0|20|0|{0|deselect|}13|19|}
{2010601001|XXGgraph_vertex_vertex|22|0|22|0|{Sort_Cust|}2|21|}
{2010602001|XXGvertex_iport_iport|24|0|24|0|{0|in|}21|23|}
{2010602001|XXGvertex_oport_oport|26|0|26|0|{0|out|}21|25|}
{2010601001|XXGgraph_vertex_vertex|28|0|28|0|{Rollup_Cust|}2|27|}
{2010602001|XXGvertex_iport_iport|30|0|30|0|{0|in|}27|29|}
{2010602001|XXGvertex_oport_oport|32|0|32|0|{0|out|}27|31|}
{2010601001|XXGgraph_vertex_vertex|34|0|34|0|{Output_Summary|}2|33|}
{2010602001|XXGvertex_iport_iport|36|0|36|0|{0|write|}33|35|}
{2010601001|XXGgraph_vertex_vertex|38|0|38|0|{Output_Rejects|}2|37|}
{2010602001|XXGvertex_iport_iport|40|0|40|0|{0|write|}37|39|}
{2010604001|XXGgraph_flow_flow|42|0|42|0|{Flow_1|}2|41|}
{2010213001|XXGoport_dst_flow|43|0|43|0|{0|}5|41|}
{2010213001|XXGiport_src_flow|44|0|44|0|{0|}9|41|}
{2010604001|XXGgraph_flow_flow|46|0|46|0|{Flow_2|}2|45|}
{2010213001|XXGoport_dst_flow|47|0|47|0|{0|}11|45|}
{2010213001|XXGiport_src_flow|48|0|48|0|{0|}15|45|}
{2010604001|XXGgraph_flow_flow|50|0|50|0|{Flow_3|}2|49|}
{2010213001|XXGoport_dst_flow|51|0|51|0|{0|}17|49|}
{2010213001|XXGiport_src_flow|52|0|52|0|{0|}23|49|}
{2010604001|XXGgraph_flow_flow|54|0|54|0|{Flow_4|}2|53|}
{2010213001|XXGoport_dst_flow|55|0|55|0|{0|}25|53|}
{2010213001|XXGiport_src_flow|56|0|56|0|{0|}29|53|}
{2010604001|XXGgraph_flow_flow|58|0|58|0|{Flow_5|}2|57|}
{2010213001|XXGoport_dst_flow|59|0|59|0|{0|}31|57|}
{2010213001|XXGiport_src_flow|60|0|60|0|{0|}35|57|}
{2010604001|XXGgraph_flow_flow|62|0|62|0|{Flow_6|}2|61|}
{2010213001|XXGoport_dst_flow|63|0|63|0|{0|}19|61|}
{2010213001|XXGiport_src_flow|64|0|64|0|{0|}39|61|}
