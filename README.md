# br_mfunds
# Project Description

Brazilian Mutual Funds Data and Analysis.

This script stores information about Brazilian investment funds in a sqlite3 database.
It is possible to make comparisons between funds and obtain information such as volatility, Sharpe Ratio, total return and annualized return.
Available benchmarks are: CDI and Bovespa Index (IBOV)

### P.S.
Please drop me a note with any feedback you have.
diegossilveira@outlook.com

All the results of this script are for information purposes and are not intended to make investment suggestions.

### Glossary
* CDI (Certificado de Depósito Interbancário) - Brazilian reference rate
* CNPJ (Cadastro Nacional de Pessoa Jurídica) - Brazilian tax ID
* IBOV - Bovespa Index
* CVM - Comissão de Valores Mobiliários

## How to run the script
Be aware that the first time you run this script, all data regarding investment funds will be downloaded, which takes some time.
The next time you run the script it will update the database with only new or updated data. **Often, existing past data is updated by the CVM.**

1. Clone the repository `git clone https://github.com/dss-Diego/br_mfunds.git`

2. Go into the folder `cd br_mfunds`

3. Install requirements `pip install -r requirements.txt`

4. Run `f_analysis.py`

## Functions
### get_fund_id()
Use this function when you want to find the fund_id using the fund name.

ie.:

`gti_id = get_fund_id()`

In the prompt type `gti dimona brasil` then select `1`
![get fund id](/get_fund_id_img.png)

The string '09.143.435/0001-60' will be assigned to the variable gti_id.

note: you can directly assign the CNPJ to the variable if you know it, without using this function.

`gti_id = '09.143.435/0001-60'`

### get_returns(fund_id, start='all', end='all')
Returns a pandas dataframe with log returns and net asset value (nav) that starts at 1.

Parameter fund_id can be:
* string with the CNPJ of the fund
* string 'cdi'
* string 'ibov'

`gti_returns = get_returns(fund_id=gti_id, start='all', end='all')`

### fund_performance(fund_id, start='all', end='all', benchmark='cdi', plot=True)
Creates two dataframes, one with the accumulated returns and the second with the performance table of the fund.

Parameter benchmark can be 'cdi' or 'ibov'

`acc_returns, performance_table = fund_performance(fund_id=gti_id, start='all', end='all', benchmark='cdi', plot=True)`

![performance table](/performance_table_fig.png)

![acc returns](/acc_returns_fig.png)

### compare(fund_ids, start='all', end='all', benchmark='cdi', best_start_date=False, plot=True)
Compare the returns, volatility and sharpe ratio of the funds.

Parameters:

* fund_ids: list with the CNPJs of the funds
* benchmark can be 'cdi' or 'ibov'
* best_start_date: boolean True or False

Forces that the start date is set to a date when all funds have quotas available.

`gti_id = '09.143.435/0001-60'`

`devant_id ='22.003.346/0001-86'`

`real_id = '10.500.884/0001-05'`

`funds = [real_id, gti_id, devant_id ]`

`acc_returns, details = compare(fund_ids=funds, start='2014-01-01', end='all', benchmark='ibov', best_start_date=False, plot=True)`

#### Plot with 'best_start_date=False':
![compare returns false best date](/compare_returns_false_best_date_fig.png)

`acc_returns, details = compare(fund_ids=funds, start='2014-01-01', end='all', benchmark='ibov', best_start_date=True, plot=True)`

#### Plot with 'best_start_date=True'
![compare returns true best date](/compare_returns_true_best_date_fig.png)
