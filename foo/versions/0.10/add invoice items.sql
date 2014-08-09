create table foo.invoice_item (
    id number not null,
    invoice_id number not null,
    constraint invoice_item_pk primary key (id) enable validate,
    constraint invoice_item_invoice_fk foreign key (id) references invoice (id) enable validate);

create sequence invoice_item_id_seq
    start with 1
    increment by 1
    minvalue 1
    nocache 
    nocycle 
    noorder;

