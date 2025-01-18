-- Exploratory analysis

SELECT *
FROM layoffs_staging2;


SELECT company, total_laid_off, date
FROM layoffs_staging2
ORDER BY total_laid_off DESC;


-- total people laid off groupped by company, industry, country, year

SELECT company, sum(total_laid_off)
FROM layoffs_staging2
GROUP  BY company
ORDER BY 2 DESC;



SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
GROUP  BY industry
ORDER BY 2 DESC;



SELECT country, sum(total_laid_off)
FROM layoffs_staging2
GROUP  BY country
ORDER BY 2 DESC;



SELECT year(date), sum(total_laid_off)
FROM layoffs_staging2
GROUP  BY year(date)
ORDER BY 1 DESC;



SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;


-- companies with percentage_laid_off 1 had 100% of people laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;








-- Rolling total

WITH rolling_sum AS(
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 
)
SELECT `month`, total_off, SUM(total_off) OVER(ORDER BY `month`) AS rolling_total
FROM rolling_sum;

-- Ranking 

WITH company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_year_rank AS (
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_year_rank
WHERE ranking <=5;






