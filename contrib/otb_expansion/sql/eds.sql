create extension eds;


insert into eds.eds_instance_table(ins_loc,ins_name,hostname,port,username,dbname,passwd,params,isactive) values
('Beijing','ins001','192.168.64.1',5432,'ins001','db01','laksjdflkj*&23jl4','alkdjflOIEKLJLSDKFHlkja','true'),
('Shanghai','ins002','192.168.64.2',5433,'ins002','db02','laksjdflkj*&23jl4','alkdjflOIEKLJLSDKFHlkja','true'),
('Fuzhou','ins003','192.168.64.4',5434,'ins003','db03','laksjdflkj*&23jl4','alkdjflOIEKLJLSDKFHlkja','true');


select * from show_node();

