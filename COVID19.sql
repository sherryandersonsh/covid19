CREATE TABLE covid_vaccinations
(
    idnum                                 SERIAL,
    iso_code                              VARCHAR(50),
    continent                             VARCHAR(50),
    locations                             VARCHAR(50),
    dates                                 DATE,
    new_tests                             INT,
    total_tests                           INT,
    total_tests_per_thousand              REAL,
    new_tests_per_thousand                REAL,
    new_tests_smoothed                    REAL,
    new_tests_smoothed_per_thousand       REAL,
    positive_rate                         REAL,
    tests_per_case                        REAL,
    tests_units                           VARCHAR(255),
    total_vaccinations                    BIGINT,
    people_vaccinated                     INT,
    people_fully_vaccinated               INT,
    new_vaccinations                      INT,
    new_vaccinations_smoothed             INT,
    total_vaccinations_per_hundred        REAL,
    people_vaccinated_per_hundred         REAL,
    people_fully_vaccinated_per_hundred   REAL,
    new_vaccinations_smoothed_per_million REAL,
    stringency_index                      REAL,
    population_density                    REAL,
    aged_70_older                         REAL,
    gdp_per_capita                        REAL,
    median_age                            REAL,
    aged_65_older                         REAL,
    extreme_poverty                       REAL,
    cardiovasc_death_rate                 REAL,
    diabetes_prevalence                   REAL,
    female_smokers                        REAL,
    male_smokers                          REAL,
    handwashing_facilities                REAL,
    hospital_beds_per_thousand            REAL,
    life_expectancy                       REAL,
    human_development_index               REAL,
    excess_mortality                      REAL
);

-- Imports the data from the .csv file into the table
COPY covid_vaccinations (
                         iso_code, continent, locations, dates, new_tests, total_tests, total_tests_per_thousand,
                         new_tests_per_thousand, new_tests_smoothed,
                         new_tests_smoothed_per_thousand, positive_rate, tests_per_case, tests_units,
                         total_vaccinations, people_vaccinated, people_fully_vaccinated,
                         new_vaccinations, new_vaccinations_smoothed, total_vaccinations_per_hundred,
                         people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred,
                         new_vaccinations_smoothed_per_million, stringency_index, population_density, median_age,
                         aged_65_older, aged_70_older, gdp_per_capita, extreme_poverty,
                         cardiovasc_death_rate, diabetes_prevalence, female_smokers, male_smokers,
                         handwashing_facilities, hospital_beds_per_thousand, life_expectancy,
                         human_development_index, excess_mortality
    )
    FROM '/Users/dataportfolioprojects/coronavirus/COVID_Vaccinations.csv'
    DELIMITER ','
    CSV HEADER;

DROP TABLE covid_deaths

CREATE TABLE covid_deaths
(
    iso_code                           VARCHAR(50),
    continent                          VARCHAR(50),
    locations                          VARCHAR(50),
    dates                              DATE,
    population                         BIGINT,
    total_cases                        INT,
    new_cases                          INT,
    new_cases_smoothed                 REAL,
    total_deaths                       INT,
    new_deaths                         INT,
    new_deaths_smoothed                REAL,
    total_cases_per_million            REAL,
    new_cases_per_million              REAL,
    new_cases_smoothed_per_million     REAL,
    total_deaths_per_million           REAL,
    new_deaths_per_million             REAL,
    new_deaths_smoothed_per_million    REAL,
    reproduction_rate                  REAL,
    icu_patients                       INT,
    icu_patients_per_million           REAL,
    hosp_patients                      INT,
    hosp_patients_per_million          REAL,
    weekly_icu_admissions              REAL,
    weekly_icu_admissions_per_million  REAL,
    weekly_hosp_admissions             REAL,
    weekly_hosp_admissions_per_million REAL
)

COPY covid_deaths (
                   iso_code, continent, locations, dates, population, total_cases, new_cases, new_cases_smoothed,
                   total_deaths, new_deaths, new_deaths_smoothed, total_cases_per_million, new_cases_per_million,
                   new_cases_smoothed_per_million, total_deaths_per_million, new_deaths_per_million,
                   new_deaths_smoothed_per_million, reproduction_rate, icu_patients, icu_patients_per_million,
                   hosp_patients, hosp_patients_per_million, weekly_icu_admissions, weekly_icu_admissions_per_million,
                   weekly_hosp_admissions, weekly_hosp_admissions_per_million
    )
    FROM '/Users/dataportfolioprojects/coronavirus/COVID_deaths.csv'
    DELIMITER ','
    CSV HEADER;

SELECT *
FROM covid_vaccinations
ORDER BY continent, locations

SELECT *
FROM covid_deaths
ORDER BY continent, locations

SELECT locations, dates, total_cases, new_cases, total_deaths, population
FROM covid_deaths
order by locations, dates

-- Looking at Total Cases vs Total Deaths
-- Shows the likelihood of dying if you contract COVID in your country
SELECT locations, dates, total_cases, total_deaths, cast(total_deaths as decimal) / total_cases * 100 as DeathPercentage
FROM covid_deaths
where locations = 'Costa Rica'
order by locations, dates desc

-- Looking at the total cases vs population
-- Shows what % of the population got COVID
SELECT locations,
       dates,
       total_cases,
       population,
       cast(total_cases as decimal) / population * 100 as COVIDInfectionPercentage
FROM covid_deaths
where locations = 'Costa Rica'
order by locations, dates desc

-- Looking at countries with highest infection rate compared to population
SELECT locations,
       population,
       max(total_cases)                                     as HighestInfectionCount,
       max(cast(total_cases as decimal) / population) * 100 as PercentPopulationInfected
FROM covid_deaths
where continent is not null -- Continent column with empty field has the name in location instead of continent. This line corrects that.
group by locations, population
order by PercentPopulationInfected desc

-- Showing countries with highest death count per population
SELECT locations,
       max(total_deaths) as TotalDeathCount
FROM covid_deaths
where continent is not null -- Continent column with empty field has the name in location instead of continent. This line corrects that.
group by locations
order by TotalDeathCount desc

-- Breaking down by continent
-- Showing the continents with the highest death count
SELECT continent,
       max(total_deaths) as TotalDeathCount
FROM covid_deaths
where continent is not null -- Continent column with empty field has the name in location instead of continent. This line corrects that.
group by continent
order by TotalDeathCount desc

-- Global Total Cases, Total Deaths and Death % per day
with variable as (
    select dates,
           sum(new_cases)  as Total_Cases,
           sum(new_deaths) as Total_Deaths
    FROM covid_deaths
    group by dates
    order by dates)

-- This SELECT is querying the output from the above query
select dates,
       total_cases,
       total_deaths,
       cast(total_deaths as decimal) / total_cases * 100 as DeathPercentage
from variable
-- to avoid the division by zero error
where Total_Cases > 0

-- Total amount of people vaccinated in the world. Total Population vs Vaccination
with PopvsVac as (
    SELECT d.continent,
           d.locations,
           d.dates,
           d.population,
           v.new_vaccinations,
           sum(new_vaccinations)
           over (partition by d.locations order by d.locations, d.dates) as RollingPeopleVaccinated
    from covid_deaths d
             join covid_vaccinations v
                  on d.locations = v.locations and d.dates = v.dates
    where d.continent is not null
    order by locations, dates)

SELECT continent,
       locations,
       dates,
       population,
       new_vaccinations,
       sum(new_vaccinations) over (partition by locations order by locations, dates) as RollingPeopleVaccinated,
       cast(RollingPeopleVaccinated as decimal) / population * 100 as RollingPeopleVaccinatedPerc
from PopvsVac

