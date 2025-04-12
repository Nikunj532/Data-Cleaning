-- DATA CLEANING

SELECT * 
FROM layoffs;

-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE DATA (SPELLING,STANDARD MISTAKES)
-- 3. NULL VALUES OR BLANK VALUES
-- 4. REMOVE ANY UNNECESSARY COLUMN

-- STEP1 - MAKE A COPY OF RAW DATA SET TABLE

CREATE TABLE layoffs_staging
select * 
from layoffs; 

insert into layoffs_staging
select * 
from layoffs; 

select * from layoffs_staging;

-- STEP2 - APPLY ROW NUMBER TO FND THE NUMBER OF DUOLCATES.

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num
from layoffs_staging ;

-- STEP3 - CREATE A CTE TO REMOVE DUPLICATE ENTRIES WHICH WLL BE DECDED BY ROW_NUM GREATER THAN 1.

WITH DUPLICATE_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num
from layoffs_staging )
SELECT * 
FROM DUPLICATE_CTE
WHERE row_num > 1;

-- checking the duplicates
select * from layoffs_staging
where company = "Casper";

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num
from layoffs_staging ;
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

delete
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2;


-- STANDARDIZING DATA

-- REMOVE UNNESCESSARY SPACES LIKE IN COMPLANY NAME

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- NOW COMPANY NAME ARE ALIGNERD PROPERLY
-- NOW WE ARE MVING TO INDUSTRY COLUMN

SELECT distinct industry from layoffs_staging2
order by 1
;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

SELECT * from layoffs_staging2
order by industry 
;

-- here we are done with industry 
-- now check location

SELECT distinct location from layoffs_staging2
order by 1
;

update layoffs_staging2
set location = 'Düsseldorf'
where location = 'DÃ¼sseldorf';

update layoffs_staging2
set location = 'Florianópolis'
where location = 'FlorianÃ³polis';

update layoffs_staging2
set location = 'Malmö'
where location = 'MalmÃ¶';

-- here we are done with location 
-- now check country

SELECT distinct country from layoffs_staging2
order by 1
;

update layoffs_staging2
set country = 'United States'
where country = 'United States.';

-- now we are done with country
-- now check date
-- we need to convert text date tot date datatype and correct format

select `date`,str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y') ;

select *
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

-- now we are done with date
-- check null or blank values

select * 
from layoffs_staging2
where industry is null or industry = '';

 select * 
from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where t1.industry is null and t2.industry is not null;


update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null; 

 select * 
from layoffs_staging2; 

-- now we have done data population in the possible rows 
-- now we will delete the rowws which have percentage_laid_off and total_laid_off as null or blank

select * from layoffs_staging2
where percentage_laid_off is null and total_laid_off is null;

delete from layoffs_staging2
where percentage_laid_off is null and total_laid_off is null;

-- now we have deleted the unnecessary data 
-- now we dont need row_num column any more so drop it

alter table layoffs_staging2
drop column row_num; 

select * 
from layoffs_staging2; 


-- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

 