--Load the link graph into table 

create table if not exists linkgraph(index String, ref String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA local inpath '/home/administrator/workspace/final_project/output' OVERWRITE INTO TABLE linkgraph;

--Define N (Total Number of Nodes)  with scaling factor

create table if not exists nodes(n Int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table nodes select count(distinct(index)) from linkgraph;

create table scale_factor(sf double) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table scale_factor select ((1-0.85)/n) from nodes;

--Creating inlinks and outlinks table

create table t1(pages String, citations String)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t1 select index, count(*) from linkgraph group by index;

create table t2(pages String, cite_inlink String)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t2 select ref, count(*) from linkgraph group by ref;

create table t3(index String, inlinks int, outlinks int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t3 select t1.pages, t2.cite_inlink, t1.citations FROM t1 JOIN t2 ON (t1.pages=t2.pages);

--Determining relative inlinks and outlinks where p1->p2

create table t4(index String, ref String, outlinks int, inlinks int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t4 select linkgraph.index, linkgraph.ref, t3.outlinks,t3.inlinks FROM linkgraph JOIN t3 ON (linkgraph.ref=t3.index);

--Determining total outlinks and inlinks where p1-> k

create table t5(index String, total_outlinks int, total_inlinks int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t5 select index, sum(outlinks), sum(inlinks) from t4 group by index;

--Combining the total outlinks(k),total inlinks(k) with outlinks(p2),inlinks(p2) for weight calculations.

create table t6(index String, ref String, total_outlinks int, total_inlinks int, outlinks_ref int, inlinks_ref int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t6 select t4.index,t4.ref,t5.total_outlinks,t5.total_inlinks,t4.outlinks,t4.inlinks from t4 join t5 on (t4.index = t5.index);

--Calculating Win and Wout

create table t7(index String, ref String, win_ref float, wout_ref float)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'; 

insert overwrite table t7 select index, ref, inlinks_ref/total_inlinks as win, outlinks_ref/total_outlinks as wout from t6;

--Assigning initial ranking 

create table initial_ranking (index String, ref String, page_rank double);

insert overwrite table initial_ranking select t7.index,t7.ref,(1/nodes.n) from t7 cross join nodes;

--Now Calculating the pagerank

--Iteration 1

create table i1(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i1 select t7.index, t7.ref, initial_ranking.page_rank, t7.win_ref, t7.wout_ref from initial_ranking join t7 on (initial_ranking.ref=t7.ref) where initial_ranking.index=t7.index;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i1 group by ref;

create table itr_pagerank(ref String, page_rank double)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration2

create table i2(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i2 select it1.index, it1.ref, itr.page_rank, it1.win_ref, it1.wout_ref from i1 it1 left outer join itr_pagerank itr on (it1.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i2 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration3

create table i3(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i3 select it2.index, it2.ref, itr.page_rank, it2.win_ref, it2.wout_ref from i2 it2 left outer join itr_pagerank itr on (it2.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i3 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration4

create table i4(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i4 select it3.index, it3.ref, itr.page_rank, it3.win_ref, it3.wout_ref from i3 it3 left outer join itr_pagerank itr on (it3.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i4 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration5

create table i5(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i5 select it4.index, it4.ref, itr.page_rank, it4.win_ref, it4.wout_ref from i4 it4 left outer join itr_pagerank itr on (it4.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i5 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration6

create table i6(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i6 select it5.index, it5.ref, itr.page_rank, it5.win_ref, it5.wout_ref from i5 it5 left outer join itr_pagerank itr on (it5.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i6 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration7

create table i7(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i7 select it6.index, it6.ref, itr.page_rank, it6.win_ref, it6.wout_ref from i6 it6 left outer join itr_pagerank itr on (it6.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i7 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration8

create table i8(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i8 select it7.index, it7.ref, itr.page_rank, it7.win_ref, it7.wout_ref from i7 it7 left outer join itr_pagerank itr on (it7.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i8 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration9

create table i9(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i9 select it8.index, it8.ref, itr.page_rank, it8.win_ref, it8.wout_ref from i8 it8 left outer join itr_pagerank itr on (it8.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i9 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

--Iteration10

create table i10(index String, ref String, page_rank double, win_ref double, wout_ref double);

insert overwrite table i10 select it9.index, it9.ref, itr.page_rank, it9.win_ref, it9.wout_ref from i9 it9 left outer join itr_pagerank itr on (it9.index=itr.ref);

drop view steps;

create view steps as select ref, sum(page_rank*win_ref*wout_ref) as summation from i10 group by ref;

insert overwrite table itr_pagerank select steps.ref, scale_factor.sf + (0.85 * summation) as page_rank from steps cross join scale_factor;

-- Final Table 

--Load the corresponding titles into the table
 
create table if not exists title_graph(index String,ref String, title String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA local inpath '/home/administrator/workspace/final_project_1/output_1' OVERWRITE INTO TABLE title_graph;

--Calculating number of citations (Total number of inlinks)

create table t_inlinks(ref String, num_citations Int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table t_inlinks select ref,count(*) from title_graph group by ref;

--Merging the number of citations with index and title

create table temp2 (index String, title String, num Int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table temp2 select distinct(title_graph.index),title_graph.title,t_inlinks.num_citations from title_graph join t_inlinks on (title_graph.index = t_inlinks.ref);

--Merging recent pagerank with the title and number of citations

create table final_pagerank(paper_title String, number_of_citations Int, page_rank double)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table final_pagerank select temp2.title, temp2.num, itr_pagerank.page_rank from temp2 join itr_pagerank on (temp2.index = itr_pagerank.ref) order by itr_pagerank.page_rank desc limit 10;

select * from final_pagerank;

--End









