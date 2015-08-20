-- unset deciles

UPDATE stats SET words_decile = 0, outlinks_decile = 0, inlinks_decile = 0;

-- looped
UPDATE stats SET words_decile = 0, outlinks_decile = 0, inlinks_decile = 0;

DROP PROCEDURE IF EXISTS set_deciles;
DELIMITER //
CREATE PROCEDURE set_deciles()
    BEGIN
    DECLARE decile INT Default 1 ;

    SET @decile_length:=(SELECT ROUND((SELECT COUNT(*) FROM stats)/100));

    PREPARE stmt_words FROM "UPDATE stats SET words_decile=? WHERE words_decile=0 ORDER BY words LIMIT ?";
    PREPARE stmt_outlinks FROM "UPDATE stats SET outlinks_decile=? WHERE outlinks_decile=0 ORDER BY outlinks LIMIT ?";
    PREPARE stmt_inlinks FROM "UPDATE stats SET inlinks_decile=? WHERE inlinks_decile=0 ORDER BY inlinks LIMIT ?";

    words_loop: LOOP

        SET @decile:=decile;

        EXECUTE stmt_words USING @decile, @decile_length;
        EXECUTE stmt_outlinks USING @decile, @decile_length;
        EXECUTE stmt_inlinks USING @decile, @decile_length;

        SET decile = decile + 1;
        IF decile <= 100 THEN
          ITERATE words_loop;
        END IF;
        LEAVE words_loop;
    END LOOP words_loop;
END//
DELIMITER ;
CALL set_deciles();


-- set deciles 10er

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

-- Set deciles 100er

SET @decile_length:=(SELECT ROUND((SELECT COUNT(*) FROM stats)/100));

PREPARE stmt FROM "UPDATE stats SET words_decile=? WHERE words_decile=0 ORDER BY words LIMIT ?";
-- PREPARE stmt FROM "UPDATE stats SET outlinks_decile=? WHERE outlinks_decile=0 ORDER BY outlinks LIMIT ?";
-- PREPARE stmt FROM "UPDATE stats SET inlinks_decile=? WHERE inlinks_decile=0 ORDER BY inlinks LIMIT ?";

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

-- 11-20
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 21-30
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 31-40
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 41-50
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 51-60
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 61-70
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 71-80
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 81-90
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;

-- 91-100
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
SET @d:= @d + 1; EXECUTE stmt USING @d, @decile_length;
