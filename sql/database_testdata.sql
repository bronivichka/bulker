insert into timezone (name, msk_offset) values ('Москва', 0);
insert into region (code, name, timezone) values (78, 'Санкт-Петербург', 1);
insert into def_code (min, max, region, operator) values (71111111111, 79999999999, 78, 'Тестовый');
insert into bulk (file, message, start_stamp, start_time, end_time, active) values ('/testdata/bulk.csv', 'Тестовое сообщение', now(), '00:00:00', '23:59:59', true);
