create table foo.invoice (
    id number not null,
    customer_id number not null,
    constraint invoice_pk primary key (id) enable validate,
    constraint invoice_customer_fk foreign key (id) references customer (id) enable validate);
