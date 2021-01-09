# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 15:34:25 2020

@author: diego
"""

import pandas as pd
import os
import sqlite3
from pandas_datareader import DataReader
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from fuzzywuzzy import process
import update_db

pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 10)

cwd = os.getcwd()
conn = sqlite3.connect(cwd + '\\data\\fundos.db')
db = conn.cursor()
# update_db.update_pipeline()
#%% functions
def get_fund_id():
    """
    Use this function when you want to find the fund_id using the fund name.

    Returns
    -------
    fund_id: string
        The CNPJ of the fund, that is the brazilian tax id and used in this 
        script as fund_id.

    """
    funds = pd.read_sql("SELECT DISTINCT denom_social FROM inf_cadastral", conn)
    funds['denom_social_query'] = funds['denom_social'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    funds_list = funds['denom_social_query'].to_list()
    x = 0
    while x == 0:
        name = input("Mutual Fund name: ")
        result = process.extract(name.upper(), funds_list, limit=5)
        for i in range(1,6):
            print(str(i)+'   '+result[i-1][0])
        fund = -1
        while fund not in range(0,6):
            query = input("Type fund number or 0 to query again: ")
            try:
                if int(query) in range(0,6):
                    fund = int(query)
                    if fund != 0:
                        x = 1
            except:
                print("Type a number from 1 to 5 to choose, or 0 to try again.")
    fund = result[fund-1][0]
    idx = funds[funds['denom_social_query'] == fund].index[0]
    fund = funds.loc[idx]['denom_social']
    fund_id = pd.read_sql(f"SELECT cnpj FROM inf_cadastral WHERE denom_social = '{fund}'", conn)
    return fund_id.values[0][0]

def get_returns(fund_id, start='all', end='all'):
    """
    Returns a pandas dataframe with log returns and net asset value (nav) that
    starts at 1.

    Parameters
    ----------
    fund_id : string
        Three options here: fund CNPJ, 'ibov', 'cdi'.
    start : string, optional
        Date formatted as '2019-12-31' or 'all'. The default is 'all'.
    end : string, optional
        Date formatted as '2019-12-31' or 'all'. The default is 'all'.

    Returns
    -------
    log_returns: pandas dataframe
        Log returns and net asset value that starts at 1.

    """
    if start == 'all':
        start = pd.to_datetime('1990-01-01')
    else:
        start = pd.to_datetime(start)
    if end == 'all':
        end = pd.to_datetime('2100-01-01')
    else:
        end = pd.to_datetime(end)
    if fund_id == 'ibov':
        returns = DataReader('^BVSP', 'yahoo', start=start+pd.DateOffset(-7), end=end )[['Adj Close']]
        returns['d_factor'] = returns['Adj Close'].pct_change().fillna(0) + 1
    elif fund_id == 'cdi':
        returns = pd.read_sql(f"SELECT date, d_factor FROM cdi WHERE date >= '{start}' AND date <= '{end}' ORDER BY date", conn, index_col='date')
    else:
        returns = pd.read_sql(f"SELECT date, quota FROM quotas WHERE cnpj = '{fund_id}' AND date >= '{start+pd.DateOffset(-7)}' AND date <= '{end}' ORDER BY date", conn, index_col='date')
        returns['d_factor'] = (returns['quota'].pct_change().fillna(0)) + 1
    returns = returns[['d_factor']]
    returns['log_return'] = np.log(returns['d_factor'])
    returns.index = pd.to_datetime(returns.index)  
    returns = returns[returns.index >= start]
    returns['nav'] = np.exp(returns['log_return'].cumsum())
    return returns[['log_return', 'nav']]

def fund_performance(fund_id, start='all', end='all', benchmark='cdi', plot=True):
    """
    Creates two dataframes, one with the accumulated returns and the second 
    with the performance table of the fund.

    Parameters
    ----------
    fund_id : string
        The CNPJ of the fund.
    start : string, optional
        Date formatted as '2019-12-31' or 'all'. The default is 'all'.
    end : string, optional
        Date formatted as '2019-12-31' or 'all'. The default is 'all'.
    benchmark : string, optional
        Benchmark used in the plot. Can be 'ibov' or 'cdi'. The default is 'cdi'.
    plot : boolean, optional
        Plot or not the results. The default is True.

    Returns
    -------
    accumulated returns : pandas dataframe
        Accumulated % returns.
    performance_table : pandas dataframe
        Performance table of the fund.

    """
    name = pd.read_sql(f"SELECT denom_social FROM inf_cadastral WHERE cnpj = '{fund_id}'", conn)
    name = name.values[0][0]   
    name = name.split()[0]
    returns = get_returns(fund_id = fund_id, start=start, end=end)
    returns = returns[['log_return']]
    returns = returns.rename(columns={'log_return': name})
    returns.index = pd.to_datetime(returns.index)
    ytd = (np.exp(returns.groupby(returns.index.year).sum())-1)*100
    performance_table = returns.iloc[:,0].groupby([returns.index.year, returns.index.strftime('%m')], sort=False).sum().unstack()
    performance_table = (np.exp(performance_table)-1)*100
    cols = performance_table.columns.tolist()
    performance_table = performance_table[sorted(cols)]
    performance_table = pd.concat([performance_table,ytd], axis=1)
    performance_table = performance_table.rename(columns={performance_table.columns[-1]: 'ytd'})
    if plot:
        acc_returns = get_returns(fund_id=benchmark, start=start, end=end)
        acc_returns = acc_returns[['log_return']]
        acc_returns = acc_returns.rename({'log_return': benchmark.upper()}, axis=1)    
        acc_returns = acc_returns.merge(returns, how='left', left_index=True, right_index=True)
        acc_returns = acc_returns.dropna()
        acc_returns = (np.exp(acc_returns.cumsum())-1)*100        
        
        fig, ax = plt.subplots(figsize=(16,9))
        sns.heatmap(ax=ax, 
                    data=performance_table, 
                    center=0, vmin=-.01, vmax=.01, 
                    linewidths=.5, 
                    cbar=False, 
                    annot=True, annot_kws={'fontsize': 10, 'fontweight': 'bold'}, fmt='.1f', 
                    cmap=['#c92d1e', '#38c93b'])
        plt.title('Performance Table - % return', fontsize=25)
        ax.set_xlabel('Month', fontsize=15)
        ax.set_ylabel('Year', fontsize=15)
        plt.show()
        
        fig, ax = plt.subplots(figsize=(16,9))
        ax.plot(acc_returns.iloc[:,0], label=acc_returns.columns[0], color='black', linestyle='--')
        ax.plot(acc_returns.iloc[:,1], label=acc_returns.columns[1])
        ax.fill_between(acc_returns.index, acc_returns.iloc[:,0], acc_returns.iloc[:,1], alpha=.25, where=(acc_returns.iloc[:,1]>acc_returns.iloc[:,0]), color='Green', label='Above Benchmark')
        ax.fill_between(acc_returns.index, acc_returns.iloc[:,0], acc_returns.iloc[:,1], alpha=.25, where=(acc_returns.iloc[:,1]<=acc_returns.iloc[:,0]), color='Red', label='Below Benchmark')
        ax.grid()
        ax.legend(loc='upper left')
        ax.set_xlabel('Date')
        ax.set_ylabel('Return (%)')
        plt.show()
    returns = (np.exp(returns.cumsum())-1)*100 
    returns = returns.rename(columns={returns.columns[-1]: 'acc_return'})
    return returns[['acc_return']], performance_table
    
def compare(fund_ids, start='all', end='all', benchmark='cdi', best_start_date=False, plot=True):
    """
    Compare the returns, volatility and sharpe ratio of the funds.

    Parameters
    ----------
    fund_ids : list of stings
        list with the CNPJs of the funds to compare.
    start : string, optional
        Date formatted as '2019-12-31' or 'all'. The default is 'all'.
    end : string, optional
        Date formatted as '2019-12-31' or 'all'. The default is 'all'.
    benchmark : TYPE, optional
        Benchmark used in the plot. Can be 'ibov' or 'cdi'. The default is 'cdi'.
    best_start_date : boolean, optional
        Forces that the start date is set to a date when all funds have quotas available. The default is False.
    plot : boolean, optional
        Plot or not the results. The default is True.

    Returns
    -------
    acc_returns : pandas dataframe
        Accumulated returns of the funds.
    details : pandas dataframe
        Volatility, Sharpe ratio, Annualized return and Total return.

    """
    acc_returns = get_returns(fund_id=benchmark, start=start, end=end)
    acc_returns = acc_returns[['log_return']]
    acc_returns = acc_returns.rename({'log_return': benchmark.upper()}, axis=1)
    for fund in fund_ids:
        name = pd.read_sql(f"SELECT denom_social FROM inf_cadastral WHERE cnpj = '{fund}'", conn)
        name = name.values[0][0]   
        name = name.split()[0]
        returns = get_returns(fund_id = fund, start=start, end=end)
        returns = returns[['log_return']]
        returns = returns.rename(columns={'log_return': name})
        returns.index = pd.to_datetime(returns.index)
        acc_returns = acc_returns.merge(returns, how='left', left_index=True, right_index=True)
    if best_start_date:
        acc_returns = acc_returns.dropna()
    corr = acc_returns.corr()
    annualized_std = acc_returns.std(axis=0)*(252**(1/2))
    total_returns = np.exp(acc_returns.sum())-1
    annualized_returns =  np.exp((acc_returns.sum() / len(acc_returns)) * 252)-1
    excess_returns = annualized_returns - annualized_returns[0]
    sharpe = excess_returns / annualized_std
    acc_returns = (np.exp(acc_returns.cumsum())-1)*100
    details = pd.concat([annualized_std*100, sharpe, annualized_returns*100, total_returns*100], axis=1)
    details.columns = ['Volatility(%)$^*$', 'Sharpe', 'Return(%)$^*$', 'Total Return(%)']
    if plot:
        fig, axs = plt.subplots(2,2, gridspec_kw={'width_ratios': [2.8, 1]}, figsize=(19.2,10.8), facecolor='grey')
        gs = axs[0, 0].get_gridspec()
        axs[0,0].remove()
        axs[1,0].remove()
        ax1 = fig.add_subplot(gs[:, 0])
        ax2 = axs[0,1]
        ax3 = axs[1,1]
        
        ax1.plot(acc_returns.iloc[:,0], label=acc_returns.columns[0], lw=1.5, color='black', linestyle='--')
        for i in range(1, len(acc_returns.columns)):
            ax1.plot(acc_returns.iloc[:,i], label=acc_returns.columns[i], lw=.75)
        ax1.set_xlabel('Date')
        ax1.set_title('% Return') 
        ax1.grid()
        ax1.plot([0.68, 0.68], [0, 0.93], color='black', lw=1,
                 transform=plt.gcf().transFigure, clip_on=False)
        ax1.plot([0.1, 0.9], [0.93, 0.93], color='black', lw=1,
                 transform=plt.gcf().transFigure, clip_on=False)
        
        ax1.legend(loc='upper left')
        plt.setp(ax1.get_xticklabels() + ax2.get_xticklabels(), rotation=30, ha='right')
        ax1.yaxis.tick_right()
        ax1.yaxis.set_label_position("right")
        ax1.set_ylabel('% Return')
        
        ax2.set_title('Correlation Matrix') 
        mask = np.zeros_like(corr)
        mask[np.triu_indices_from(mask)] = True  
        sns.heatmap(
            corr.iloc[1:,:-1], 
            mask=mask[1:,:-1],
            ax=ax2,
            vmin=-1, vmax=1, center=0,
            cmap=sns.diverging_palette(20, 220, n=200),
            square=True,cbar=False, annot=True, annot_kws={'fontsize': 10}, fmt='.2f')
        ax2.set_facecolor("grey")
        ax2.set_xticklabels(ax2.get_xmajorticklabels(), fontsize = 9)
        ax2.set_yticklabels(ax2.get_ymajorticklabels(), fontsize = 9)
        
        ax3.set_title("Details")
        sns.heatmap(
            details, 
            ax=ax3,
            linewidths = .5, linecolor='black',
            vmin=0, vmax=0, center=0,
            cmap=['#5b7273'],
            square=True,cbar=False, annot=True, annot_kws={'fontsize': 10}, fmt='.2f')
        ax3.annotate('* Annualized', (0.1, -0.25),
                       xycoords='axes fraction', va='center')
        ax3.set_yticklabels(ax3.get_ymajorticklabels(), fontsize = 9, rotation=30)
        ax3.set_xticklabels(ax3.get_xmajorticklabels(), fontsize = 9, rotation=30)
        
        fig.suptitle('Mutual Funds Performance Comparison', fontsize=16)
        plt.show()
    details.columns = ['Volatility(%) annualized', 'Sharpe', 'Return(%) annualized', 'Total Return(%)']
    return acc_returns, details

