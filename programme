/*****************************************************************
Creation d une macro SAS pour charger des donnees du serveur Hadoop vers
le serveur SAS en assignant la bonne longueur a chaque variable.
/***************************************************************/

%macro char_length(libref=,table=,schema=);
/*copier la structure de la table*/
data msp_rec.&table.;
	set &libref..&table.(where=(1=2));
	run;
proc contents data=msp_rec.&table. out=_content1; run;
data test1;
set _content1(where=(type=2));
run;

data _null_;
set test1 end=last;
call symput ('nn'||left(_n_),name); 
IF last then call symputx('cnt',_n_);;
run;
proc sql;
	CREATE TABLE MyTable(  
		nom 	varchar(20),
   		taille  num
	);
quit;
%do i=1 %to &cnt; 
%put *----------------------------------------------------------------------;
%put Etape &i. ;
%put Variable_traitee : &&nn&i. ;
%put *----------------------------------------------------------------------;
/* Calculer la taille maximale de chaque colonne de la table avec le moteur impala */
proc sql;
connect to impala(&connect_impala.);
%drop_create_table_hadoop(mot=IMPALA,table=workspacesas.test,format=parquet)
	select concat('' ,"&&nn&i.") as name   
	,case when max(length(&&nn&i.)) is null then 0
     else  max(length(&&nn&i.))
	 end as val 
	from &schema..&table.
)by impala;
disconnect from impala;
quit;
/*Stocker les resultats dans une table */
proc sql;
insert into MyTable select * from WRKVTE.test;
quit;
proc sql noprint;
select taille into : list1 separated by ',' from MyTable ;
quit;
%put *----------------------------------------------------------------------;
%put &list1.;
%put *----------------------------------------------------------------------;
/* Modifier la taille de chaque colonne */
proc sql ;
alter table msp_rec.&table.
	modify 	&&nn&i char(%scan("&list1" , &i.,",")) 
			format=$%sysfunc(trim(%scan("&list1" , &i.,","))). 
			informat=$%sysfunc(trim(%scan("&list1" , &i.,","))).  ;
quit;
%end;
/*Charger les donnees du serveur hadoop vers le serveur SAS en assignant la bonne longueur a chaque variable */
proc sql;
  insert into  msp_rec.&table. select * from &libref..&table. ;
quit;
%mend;
