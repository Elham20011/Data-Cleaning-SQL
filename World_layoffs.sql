-- Data cleaning

SELECT *
FROM layoffs;


-- 1. remove duplicate 
-- 2. standardize the data
-- 3. null or blank 
-- 4. remove any colums not necessry 


CREATE TABLE layoffs_stagging 
like layoffs;

SELECT *
FROM layoffs_stagging ;


insert layoffs_stagging
select*
from layoffs;


-- duplicates

SELECT *,
row_number() over( partition by company , location ,industry, total_laid_off, percentage_laid_off, 'data', stage , country, funds_raised_millions)
FROM layoffs_stagging 
;

with duplicates_cte as
(SELECT *,
row_number() over( partition by company , location ,industry, total_laid_off, percentage_laid_off, 'data', stage , country, funds_raised_millions) as row_num
FROM layoffs_stagging 
)
select *
from duplicates_cte 
where row_num > 1 ;

select* 
from layoffs_stagging 
where company = '2TM';


with duplicates_cte as
(SELECT *,
row_number() over( partition by company , location ,industry, total_laid_off, percentage_laid_off, 'data', stage , country, funds_raised_millions) as row_num
FROM layoffs_stagging 
)
 delete
from duplicates_cte 
where row_num > 1 ;


 
 CREATE TABLE `layoffs_stagging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_numb`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select* 
from layoffs_stagging3
where row_num > 1;

insert into layoffs_stagging3
SELECT *,
row_number() over( partition by company , location ,industry, total_laid_off, percentage_laid_off, 'data', stage , country, funds_raised_millions) as row_numb
FROM layoffs_stagging ;

DELETE
from layoffs_stagging3
where row_numb > 1;

select* 
from layoffs_stagging3;

-- STANDARDIZING DATA

SELECT company, trim(COMPANY)
FROM LAYOFFS_STAGGING3;

update LAYOFFS_STAGGING3
set company=trim(COMPANY);

SELECT distinct industry
FROM LAYOFFS_STAGGING3
;

SELECT *
FROM LAYOFFS_STAGGING3
where industry like 'crypto%';

update LAYOFFS_STAGGING3
set industry= 'crypto'
where industry like 'crypto' ;


select distinct country
from layoffs_stagging3
order by 1 ;

select distinct country, trim(trailing '.' from country)
from layoffs_stagging3
order by 1 ;

update layoffs_stagging3
set country=trim(trailing '.' from country)
where country like 'united state%';

select *
from layoffs_stagging3
;

select `date`, str_to_date('date', '%m/%d/%Y')
from layoffs_stagging3
;

select `date`
from layoffs_stagging3;

select *
from layoffs_stagging3;


update layoffs_stagging3
set `date` = str_to_date(`date`, '%m/%d/%Y');


alter table layoffs_stagging3
modify column `date` DATE;

-- NULL AND BLANCK


select *
from layoffs_stagging3
WHERE total_laid_off is null
and percentage_laid_off is null
;


select *
from layoffs_stagging3
where industry is null
or industry='';

select *
from layoffs_stagging3
where company like 'ball%';

select t1.industry , t2.industry
from layoffs_stagging3 t1
join layoffs_stagging3 t2
 on t1.company=t2.company
 and t1.location=t2.location 
 where (t1.industry is null or t1.industry ='')
 and t2.industry is not null
;

update layoffs_stagging3
set industry= null
where industry='';


update layoffs_stagging3 t1
join layoffs_stagging3 t2
on t1.company =t2.company
set t1.industry=t2.industry
where t1.industry is null 
 and t2.industry is not null;
 
 
 select *
 from layoffs_stagging3;

 
 
 select *
from layoffs_stagging3
WHERE total_laid_off is null
and percentage_laid_off is null
;
  
delete 
from layoffs_stagging3
 WHERE total_laid_off is null
and percentage_laid_off is null
;

alter table layoffs_stagging3
drop column row_numb;


 
 