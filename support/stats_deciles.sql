
SET @decile_length:=(SELECT ROUND((SELECT COUNT(*) FROM stats)/10));

PREPARE stmt FROM "UPDATE stats SET words_decile=? WHERE words_decile=0 ORDER BY words LIMIT ?";

SET @d:= 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= 2; EXECUTE stmt USING @d, @decile_length;
SET @d:= 3; EXECUTE stmt USING @d, @decile_length;
SET @d:= 4; EXECUTE stmt USING @d, @decile_length;
SET @d:= 5; EXECUTE stmt USING @d, @decile_length;
SET @d:= 6; EXECUTE stmt USING @d, @decile_length;
SET @d:= 7; EXECUTE stmt USING @d, @decile_length;
SET @d:= 8; EXECUTE stmt USING @d, @decile_length;
SET @d:= 9; EXECUTE stmt USING @d, @decile_length;
SET @d:= 10; EXECUTE stmt USING @d, @decile_length;

PREPARE stmt FROM "UPDATE stats SET outlinks_decile=? WHERE outlinks_decile=0 ORDER BY outlinks LIMIT ?";

SET @d:= 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= 2; EXECUTE stmt USING @d, @decile_length;
SET @d:= 3; EXECUTE stmt USING @d, @decile_length;
SET @d:= 4; EXECUTE stmt USING @d, @decile_length;
SET @d:= 5; EXECUTE stmt USING @d, @decile_length;
SET @d:= 6; EXECUTE stmt USING @d, @decile_length;
SET @d:= 7; EXECUTE stmt USING @d, @decile_length;
SET @d:= 8; EXECUTE stmt USING @d, @decile_length;
SET @d:= 9; EXECUTE stmt USING @d, @decile_length;
SET @d:= 10; EXECUTE stmt USING @d, @decile_length;

PREPARE stmt FROM "UPDATE stats SET inlinks_decile=? WHERE inlinks_decile=0 ORDER BY inlinks LIMIT ?";

SET @d:= 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= 2; EXECUTE stmt USING @d, @decile_length;
SET @d:= 3; EXECUTE stmt USING @d, @decile_length;
SET @d:= 4; EXECUTE stmt USING @d, @decile_length;
SET @d:= 5; EXECUTE stmt USING @d, @decile_length;
SET @d:= 6; EXECUTE stmt USING @d, @decile_length;
SET @d:= 7; EXECUTE stmt USING @d, @decile_length;
SET @d:= 8; EXECUTE stmt USING @d, @decile_length;
SET @d:= 9; EXECUTE stmt USING @d, @decile_length;
SET @d:= 10; EXECUTE stmt USING @d, @decile_length;