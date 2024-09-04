-- DATA CLEANING 
 
 
 
 -- REMOVING DUPLICATES 
 
SELECT *
FROM layoffs; 

CREATE TABLE Layoffs_staging
LIKE layoffs; 



SELECT *
FROM  Layoffs_staging; 

INSERT Layoffs_staging
SELECT *
FROM Layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;



WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;


SELECT *
FROM Layoffs_staging 
WHERE COMPANY = 'Casper';


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;


CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL, 
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,`row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT *
FROM layoffs_staging3;




INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;



DELETE 
FROM layoffs_staging3
WHERE row_num > 1;

SELECT *
FROM layoffs_staging3;


 -- STANDARDISING DATA
 

SELECT Company, (TRIM(company))
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET company = (TRIM(company));

SELECT *
FROM layoffs_staging3
WHERE industry LIKE 'crypto%'; 
 
 UPDATE layoffs_staging3
 SET industry = 'Crypto'
 WHERE industry LIKE 'Crypto%' 
 ;
 
 SELECT DISTINCT Country, TRIM(TRAILING '.' FROM Country)
 FROM layoffs_staging3
 ORDER BY 1;
 
 

 UPDATE  layoffs_staging3
 SET COUNTRY = TRIM(TRAILING '.' FROM Country)
 WHERE COUNTRY LIKE 'United states%' ;
 
 SELECT `date`,
 STR_TO_DATE(`date`, '%m/%d/%Y')
 FROM layoffs_staging3;
 
 UPDATE layoffs_staging3
 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
 
 
Select `date`
FROM layoffs_staging3;
 

 ALTER TABLE layoffs_staging3
 MODIFY COLUMN `date` DATE;
 

 
  -- Null and Blank Values
 
SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
 
 UPDATE layoffs_staging3
 SET industry = NULL
 WHERE industry = '';
 
SELECT *
FROM layoffs_staging3
WHERE industry is NULL
OR industry = '' ;

SELECT *
FROM layoffs_staging3
WHERE company = 'airbnb' ;


SELECT t1.industry, t2.industry
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
  ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.Industry IS NOT NULL; 

UPDATE layoffs_staging3 t1
JOIN layoffs_staging3 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.Industry IS NOT NULL; 




 -- REMOVE ROW AND COMUMNS
 
 SELECT *
FROM layoffs_staging3;
 
 SELECT *
 FROM layoffs_staging3
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
  DELETE 
 FROM layoffs_staging3
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 
 SELECT *
FROM layoffs_staging3;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num; 


 