-- ParserTest:sanityTest
SELECT `id`, `name` FROM `product` AS `DataSection` WHERE `DataSection`.`id` IN ('2', '14', '13', '6', '5')
-- ParserTest: insertDjango
INSERT INTO `django_session` (`session_key`, `session_data`, `expire_date`) VALUES ('tqbz3o7dvpdergag7ls6nvmlnj2fnfjb', 'Zjg3MjM0MTdkODRmNDk1ZmIzZmYyMGM4MDVjNGY4MmNkNDE1YTBjNjqAAn1xAVgPAAAAX3Nlc3Npb25fZXhwaXJ5cQJKAHUSAHMu', '2019-03-12 09:02:50.287595');
-- ParserTest: updateDjango
UPDATE `django_session` SET `session_data` = 'NDUzYjc0ZmZhYTU0YzYxZDhlZjQ4YzQyMjYzOGFm', `expire_date` = '2019-04-08 15:04:57.832310' WHERE `django_session`.`session_key` = 'od8c8hiweja7srrh3ue8yi0mkd2dhl6u'
-- ParserTest: deleteDjango
DELETE FROM `django_session` WHERE `django_session`.`session_key` IN ('yo983e7woyuek1fcfopm0xgpnmrmdzbc')
