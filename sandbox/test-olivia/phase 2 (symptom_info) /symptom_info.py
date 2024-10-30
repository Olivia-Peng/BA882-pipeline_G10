from google.cloud import bigquery
import functions_framework
import requests
from bs4 import BeautifulSoup
import pandas as pd

# BigQuery configurations
project_id = "ba882-pipeline-olivia"
dataset_id = "CDC"
table_id = f"{project_id}.{dataset_id}.symptom"

# List of diseases to scrape
diseases = ["chlamydia", "gonorrhea"]
base_url = "https://www.cdc.gov/{}/about/index.html"

# Function to scrape specific sections for a disease
def scrape_disease_info(disease_name):
    url = base_url.format(disease_name)
    response = requests.get(url)
    results = {"Disease": disease_name}  # Initialize with disease name for each entry

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. Overview Section
        overview_section = soup.find('h2', text="Overview")
        if overview_section:
            results["Overview"] = overview_section.find_next('p').get_text()
        else:
            results["Overview"] = "Not found"
        
        # 2. How it Spreads Section
        spreads_section = soup.find('h2', text="How it spreads")
        if spreads_section:
            results["How it Spreads"] = spreads_section.find_next('p').get_text()
        else:
            results["How it Spreads"] = "Not found"
        
        # 3. Symptoms Sections
        signs_symptoms_section = soup.find('h2', text="Signs and symptoms")
        if signs_symptoms_section:
            # Symptoms in Women
            symptoms_women_section = signs_symptoms_section.find_next('h4', text="Symptoms in women")
            if symptoms_women_section:
                symptoms_women_list = symptoms_women_section.find_next('ul').find_all('li')
                results["Symptoms in Women"] = "\n".join([f"{i+1}. {li.get_text()}" for i, li in enumerate(symptoms_women_list)])
            else:
                results["Symptoms in Women"] = "Not found"
            
            # Symptoms in Men
            symptoms_men_section = signs_symptoms_section.find_next('h4', text="Symptoms in men")
            if symptoms_men_section:
                symptoms_men_list = symptoms_men_section.find_next('ul').find_all('li')
                results["Symptoms in Men"] = "\n".join([f"{i+1}. {li.get_text()}" for i, li in enumerate(symptoms_men_list)])
            else:
                results["Symptoms in Men"] = "Not found"
            
            # Symptoms from Rectal Infections
            symptoms_rectal_section = signs_symptoms_section.find_next('h4', text="Symptoms from rectal infections")
            if symptoms_rectal_section:
                symptoms_rectal_list = symptoms_rectal_section.find_next('ul').find_all('li')
                results["Symptoms from Rectal Infections"] = "\n".join([f"{i+1}. {li.get_text()}" for i, li in enumerate(symptoms_rectal_list)])
            else:
                results["Symptoms from Rectal Infections"] = "Not found"
        else:
            results["Symptoms in Women"] = "Signs and symptoms section not found"
            results["Symptoms in Men"] = "Signs and symptoms section not found"
            results["Symptoms from Rectal Infections"] = "Signs and symptoms section not found"
        
        # 4. Prevention Section
        prevention_section = soup.find('h2', text="Prevention")
        if prevention_section:
            prevention_text = prevention_section.find_next('p').get_text()
            if "The only way to completely avoid STIs is to not have vaginal, anal, or oral sex." in prevention_text:
                results["Prevention"] = "The only way to completely avoid STIs is to not have vaginal, anal, or oral sex."
            else:
                results["Prevention"] = "Specific prevention sentence not found"
        else:
            results["Prevention"] = "Prevention section not found"
        
        # 5. Treatment and Recovery Section
        treatment_section = soup.find('h2', text="Treatment and recovery")
        if treatment_section:
            results["Treatment and Recovery"] = treatment_section.find_next('p').get_text()
        else:
            results["Treatment and Recovery"] = "Not found"
    else:
        print(f"Failed to retrieve information for {disease_name}. Status code: {response.status_code}")
        results = {key: "Not found" for key in ["Disease", "Overview", "How it Spreads", "Symptoms in Women", 
                                                 "Symptoms in Men", "Symptoms from Rectal Infections", 
                                                 "Prevention", "Treatment and Recovery"]}
    return results

# Function to store data in BigQuery
def store_data_in_bigquery(data):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table("symptom")
    
    # Define the rows to insert
    rows_to_insert = [
        {
            "Disease": data["Disease"],
            "Overview": data["Overview"],
            "How it Spreads": data["How it Spreads"],
            "Symptoms in Women": data["Symptoms in Women"],
            "Symptoms in Men": data["Symptoms in Men"],
            "Symptoms from Rectal Infections": data["Symptoms from Rectal Infections"],
            "Prevention": data["Prevention"],
            "Treatment and Recovery": data["Treatment and Recovery"]
        }
    ]
    
    # Insert rows into the BigQuery table
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"Failed to insert rows: {errors}")
    else:
        print("Data successfully stored in BigQuery.")

# Main function to handle HTTP request and run scraping/storing process
@functions_framework.http
def run_scraping_to_bigquery(request):
    try:
        # Loop through each disease and scrape data
        for disease in diseases:
            disease_info = scrape_disease_info(disease)
            store_data_in_bigquery(disease_info)  # Store each disease info in BigQuery

        return "Data scraping and storing completed successfully.", 200
    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500
