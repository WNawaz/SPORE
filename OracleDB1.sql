//Direct Similarity/Distance***********************************************************
insert into aedges (select t2.fid,t2.tid, t2.fid, log ( 10, 1+ 1/(t2.elabel*t1.sim)) from (select fid, 1/count(*) as sim from tedges group by (fid))t1, tedges t2 where t1.fid=t2.fid);
CREATE TABLE TEMP as select fid, tid, pid, 1.0*(val-minVal)/valRange as val from ( select fid, tid, pid, val, min(val) over() as minVal, max(val) over () - min(val) over () as valRange from AEDGES);
CREATE TABLE AEDGES as SELECT * from TEMP;
//centroid initialization***************************************************************
INSERT INTO CENTROIDS SELECT rownum, fid,1 FROM (SELECT fid, COUNT(*) AS degree FROM TEDGES GROUP BY fid ORDER BY degree ASC ) WHERE degree>=2 AND rownum  <=10;
*********
//E-Step
*********
//CentroidsClusterMembership (CorrelatedUpdateWithMergeStatement)***************************************************************
merge into NCLUS target using CENTROIDS src on (src.nid=target.nid) when matched then update set target.clus_id=src.cid;
//SimCompFromCentroidsToOthers *************************************************************************************************
TVISITED <- SSSP(CENTROIDS,AEDGES)
desc tvisited;
insert into NSIM select fid,nid,d2s from TVISITED where fid!=nid and nid not in (select nid from CENTROIDS);
//ClusterMembershipForNonCentroids
insert into NSIMMIN select src, dest, val from (select ROW_NUMBER() over (partition by dest order by val asc) as rid, src, dest, val from NSIM) where rid=1;
merge into NCLUS target using (select nsm.dest,c.cid from NSIMMIN nsm, CENTROIDS c where nsm.src=c.nid) src on (target.nid = src.dest) when matched then update set target.clus_id = src.cid;

*********
//M-Step
*********
//ForEachCluster...
//ComputeUpperBoundFromCurrentCentroid*********************************************************************************************
select nid from CENTROIDS where cid=1;
UBOUND <- select avg(val) as avgVal from NSIM where src=5 and dest in (select nid from NCLUS where clus_id=1);
//ConstructSubGraph***************************************************************
insert into CEDGES select ae.fid, ae.tid, ae.pid, ae.val from AEDGES ae where ae.fid in (select nid from NCLUS where clus_id=1) and ae.tid in (select nid from NCLUS where clus_id=1);
//SimComAllPair
TVISITED <- SSSP(NCLUSi,CEDGES)
//CheckForPotentialNewCentroids****************************************************
(PCENTROID,VAL) <- select fid, avgVal from (select fid, avg(d2s) as avgVal from TVISITED group by fid order by avgVal asc) where rownum=1;
//UpdateThePotentialCentroidinCENTROIDTable******************************************
update CENTROIDS set nid = 5,flag=1 where cid =1;
***************
//ExtractSPORE
***************
//FromRootNodes
merge into TOUTSEGS target using (SELECT tv.fid as fid, tv.nid as tid, tv.p2s as pid, tv.d2s as cost from TVISITED tv where tv.p2s!=tv.fid and tv.d2s<=0.5) src on ( target.fid=src.fid and target.tid=src.tid) when matched then update set target.cost=src.cost, target.pid=src.pid where target.cost > src.cost when not matched then insert (target.fid, target.tid,target.pid,target.cost)  values(src.fid, src.tid, src.pid, src.cost);
//FromNonRootNodes
/picNonRootNodes
CREATE table ek(nid, p2s, dist, rnum,flag, srcid, rid) AS SELECT tv.nid, tv.p2s, ae.val, cast(1 as number), cast(2 as number), tv.p2s, tv.fid from TVISITED tv, AEDGES ae where tv.p2s=ae.fid and tv.nid=ae.tid and tv.p2s!=tv.fid and ae.val<=0.5;
/Expand
CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid,rid) AS SELECT * FROM (SELECT ae.nid, ae.p2s, cast(ae.dist+q.dist as float) dst, cast(ROW_NUMBER() over (partition by q.rid, ae.nid order by ae.dist+q.dist asc) as number) rnum,cast(0 as number), q.p2s as src,q.rid FROM ek q, ek ae WHERE q.nid=ae.p2s and q.rid=ae.rid and ae.flag=2) temp WHERE temp.rnum=1;

