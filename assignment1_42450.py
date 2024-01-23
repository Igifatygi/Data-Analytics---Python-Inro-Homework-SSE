#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Group member:
    42441; 42450
"""
#First, change the working path
import os
path = os.path.split(os.path.abspath(__file__))[0]
os.chdir(path)

#%%
import sqlalchemy as sql
import pandas as pd
'''Step 1 - Amortisation Rate'''
def amort_rate(property_valuation, loan_size, gross_yearly_income):
    "Giving property_valuation, loan_size, gross_yearly_income ,\
        the function calculates the amortisation rate of a household"
    loan_to_value_ratio = loan_size/property_valuation
    if loan_to_value_ratio>0.7:
        amortisation_rate = 0.02
    elif 0.5<loan_to_value_ratio<=0.7:
        amortisation_rate = 0.01
    else:
        amortisation_rate = 0
    amortisation_rate += 0.01*(loan_size>4.5*gross_yearly_income)
    return amortisation_rate

'''Step 2 - Children'''
def child_cost(children_number):
    "Giving the number of childrens, returns the total anticipated cost per year."
    if children_number == 0:
        monthly_support = 0
    elif children_number == 1:
        monthly_support = 1250
    elif children_number == 2:
        monthly_support = 2650
    elif children_number == 3:
        monthly_support = 4480
    elif children_number == 4:
        monthly_support = 6740
    elif children_number == 5:
        monthly_support = 9240
    elif children_number == 6:
        monthly_support = 11740
    elif children_number > 6:
        monthly_support = 11740 + 1250*(children_number-6)
    children_cost = 12*(3700*children_number-monthly_support)
    return children_cost

'''Step 3 - Calculate Taxes'''
'''    Frist download the municipality tax data from sql
       The code is noted, no need to run a second time'''
#host, username, password, schema = "mysql-1.cda.hhs.se", "7313", "data" ,"kalp"
#connection_string = "mysql+pymysql://{}:{}@{}/{}".format(username, password, host, schema)
#connection = sql.create_engine(connection_string)
#tax_query = "SELECT * FROM TaxRate"
#tax_df = pd.read_sql_query(con=connection.connect(), sql=sql.text(tax_query))
#tax_dictionary = tax_df.set_index('municipality', drop=True)['tax_rate'].to_dict()
#print(tax_dictionary)
'''The result:
    {'Botkyrka': 32.23, 'Danderyd': 30.43, 'Ekerö': 31.03, 'Haninge': 31.76,\
     'Huddinge': 31.55, 'Järfälla': 31.07, 'Lidingö': 29.92, 'Nacka': 30.06,\
     'Norrtälje': 31.8, 'Nykvarn': 32.05, 'Nynäshamn': 31.93, 'Salem': 31.5,\
     'Sigtuna': 32.08, 'Sollentuna': 30.2, 'Solna': 29.45, 'Stockholm': 29.82,\
     'Sundbyberg': 31.33, 'Södertälje': 32.23, 'Tyresö': 31.58, 'Täby': 29.63,\
     'Upplands Väsby': 31.5, 'Upplands-Bro': 31.48, 'Vallentuna': 30.98,\
     'Vaxholm': 31.38, 'Värmdö': 31.06, 'Österåker': 28.98}'''

def tax(municipality,gross_yearly_income):
    "Giving the municipality and gross yearly income, returns the taxes paid"
    tax_dictionary = {'Botkyrka': 32.23, 'Danderyd': 30.43, 'Ekerö': 31.03, 'Haninge': 31.76,\
      'Huddinge': 31.55, 'Järfälla': 31.07, 'Lidingö': 29.92, 'Nacka': 30.06,\
      'Norrtälje': 31.8, 'Nykvarn': 32.05, 'Nynäshamn': 31.93, 'Salem': 31.5,\
      'Sigtuna': 32.08, 'Sollentuna': 30.2, 'Solna': 29.45, 'Stockholm': 29.82,\
      'Sundbyberg': 31.33, 'Södertälje': 32.23, 'Tyresö': 31.58, 'Täby': 29.63,\
      'Upplands Väsby': 31.5, 'Upplands-Bro': 31.48, 'Vallentuna': 30.98,\
      'Vaxholm': 31.38, 'Värmdö': 31.06, 'Österåker': 28.98}
    municipality_tax = gross_yearly_income*tax_dictionary[municipality]*0.01
    state_tax = max(0,(gross_yearly_income-554900)*0.2)
    return round(municipality_tax+state_tax)

'''Step 4 - Calculate Disposable Income'''
def disposable_income(customer):
    "Calculate the disposable income of customers"
    taxes_paid = tax(customer["municipality"],customer["gross_yearly_income"])
    amortization = amort_rate(customer["property_valuation"],customer["requested_loan"]\
                              ,customer["gross_yearly_income"])*customer["requested_loan"]
    children_cost = child_cost(customer["num_children"])
    living_cost = 9700*12
    interest_cost = customer["requested_loan"]*0.065
    housing_cost = 4000*12 if customer["housing_type"] == "apartment" else 4500*12
    return round(customer["gross_yearly_income"]-taxes_paid-amortization\
                 -children_cost-living_cost-interest_cost-housing_cost)
        
'''Step 5 - Plugging in the customers'''
#Get the data from sql - I anonymize that with "XXXXXX"
host, username, password, schema = "XXXXXXXX", "XXXXX", "XXXXXX" ,"XX"
connection_string = "mysql+pymysql://{}:{}@{}/{}".format(username, password, host, schema)
connection = sql.create_engine(connection_string)
customer_query = "SELECT * FROM Customer"
customer_df = pd.read_sql_query(con=connection.connect(), sql=sql.text(customer_query), index_col='customer_id')
#Generate the "disposable_income" column using apply function.
customer_df["disposable_income"] = customer_df.apply(lambda row: disposable_income(row.to_dict()), axis=1)

'''Step 6 - Cost of servicing existing loans'''
loan_query = "SELECT * FROM CustomerLoan"
loan_df = pd.read_sql_query(con=connection.connect(), sql=sql.text(loan_query), index_col='loan_id')
loan_df['existing_loan_cost'] = loan_df['amount']*loan_df['interest_rate']
loan_df = loan_df.groupby('customer_id')['existing_loan_cost'].sum()
customer_df = pd.concat([customer_df,loan_df], axis=1)
customer_df.fillna(0,inplace=True)

def disposable_income_after_loans(customer):
    "Calculate the disposable income of customers taking account of existing loans"
    taxes_paid = tax(customer["municipality"],customer["gross_yearly_income"])
    amortization = amort_rate(customer["property_valuation"],customer["requested_loan"]\
                              ,customer["gross_yearly_income"])*customer["requested_loan"]
    children_cost = child_cost(customer["num_children"])
    living_cost = 9700*12
    interest_cost = customer["requested_loan"]*0.065
    housing_cost = 4000*12 if customer["housing_type"] == "apartment" else 4500*12
    existing_loan_cost = customer["existing_loan_cost"]
    return round(customer["gross_yearly_income"]-taxes_paid-amortization\
                 -children_cost-living_cost-interest_cost-housing_cost-existing_loan_cost)
customer_df["disposable_income"] = customer_df.apply(lambda row: disposable_income_after_loans(row.to_dict()), axis=1)

'''Step 7 - How Big Can the Mortgage be'''
def max_martgage(customer):
    "Calculate the Max Mortgage, with the precision of 1000."
    for i in range(0, int(customer["property_valuation"]*0.85+1), 1000):
        customer['requested_loan'] = i
        customer_disposable_income_after_loans = disposable_income_after_loans(customer)
        if customer_disposable_income_after_loans <= 0:
            return i-1000 if i>0 else 0
    return i
customer_df["max_loan"] = customer_df.apply(lambda row: max_martgage(row.to_dict()), axis=1)

'''Step 8 - Storing the output'''
answer_df = customer_df
answer_df.rename(columns={'existing_loan_cost':'answer_existing_loans_cost',
                          'disposable_income':'answer_disposable_income',
                          'max_loan':'answer_max_loan'}, inplace=True)
answer_df["answer_amortization"] =\
    answer_df.apply(lambda row: amort_rate(row['property_valuation'],\
    row['requested_loan'],row['gross_yearly_income']), axis=1)     
answer_df["answer_total_child_cost"] =\
    answer_df.apply(lambda row: child_cost(row['num_children']), axis=1)
answer_df["answer_taxes"] = answer_df.apply(lambda row: tax(row['municipality'],\
    row['gross_yearly_income']), axis=1)
answer_df.reset_index(inplace=True)
answer_df = answer_df[['customer_id','answer_amortization','answer_total_child_cost',\
                       'answer_taxes','answer_existing_loans_cost',\
                       'answer_disposable_income','answer_max_loan']]
#%% Write df to a JSON file(for both group member)
answer_df.to_json("assignment1_42441.json", orient="records", indent=2)
answer_df.to_json("assignment1_42450.json", orient="records", indent=2)