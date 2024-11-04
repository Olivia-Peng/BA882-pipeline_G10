from google.cloud import bigquery
import functions_framework

# BigQuery configurations
project_id = "ba882-pipeline-olivia"
dataset_id = "CDC"
table_id = "symptom"

# List of diseases and their predefined text for each section
predefined_data = {
    "Campylobacter": {
        "Overview": (
            "Campylobacter are bacteria that can make people ill with diarrhea. The illness is called campylobacteriosis. "
            "Campylobacter cause the most bacterial diarrheal illnesses in the United States. CDC estimates that 1.5 million people in the United States get ill from Campylobacter every year."
        ),
        "How it Spreads": (
            "Campylobacter can live in the intestines, liver, and other organs of animals. Many chickens, cows, and other animals carry Campylobacter without becoming sick. "
            "Campylobacter can spread from these animals to people."
        ),
        "Symptoms in Women": "N/A",
        "Symptoms in Men": "N/A",
        "Symptoms from Rectal Infections": "N/A",
        "Prevention": "N/A",
        "Treatment and Recovery": "N/A"
    },
    "Chlamydia": {
        "Overview": (
            "Chlamydia is a common STI that can cause infection among men and women. "
            "It can cause permanent damage to a woman's reproductive system. This can make it difficult or impossible to get pregnant later. "
            "Chlamydia can also cause a potentially fatal ectopic pregnancy (pregnancy that occurs outside the womb)."
        ),
        "How it Spreads": (
            "You can get chlamydia by having vaginal, anal, or oral sex without a condom with someone who has the infection. "
            "A pregnant person with chlamydia can give the infection to their baby during childbirth."
        ),
        "Symptoms in Women": (
            "Even when chlamydia has no symptoms, it can damage a woman's reproductive system. "
            "Women with symptoms may notice: An abnormal vaginal discharge, A burning sensation when peeing."
        ),
        "Symptoms in Men": (
            "Men with symptoms may notice: A discharge from their penis, A burning sensation when peeing, "
            "Pain and swelling in one or both testicles (although this is less common)."
        ),
        "Symptoms from Rectal Infections": (
            "Men and women can also get chlamydia in their rectum. "
            "This happens either by having receptive anal sex, or by spread from another infected site (such as the vagina). "
            "While these infections often cause no symptoms, they can cause: Rectal pain, Discharge, Bleeding."
        ),
        "Prevention": (
            "The only way to completely avoid STIs is to not have vaginal, anal, or oral sex. "
            "If you are sexually active, the following things can lower your chances of getting chlamydia: "
            "Being in a long-term mutually monogamous relationship with a partner who has been tested and does not have chlamydia, "
            "Using condoms the right way every time you have sex."
        ),
        "Treatment and Recovery": (
            "Yes, the right treatment can cure chlamydia. It is important that you take all of the medicine your healthcare provider gives you to cure your infection. "
            "Do not share medicine for chlamydia with anyone. When taken properly it will stop the infection and could decrease your chances of having problems later. "
            "Although medicine will stop the infection, it will not undo any permanent damage caused by the disease. "
            "Repeat infection with chlamydia is common. You should receive testing again about three months after your treatment, even if your sex partner(s) receives treatment."
        )
    },
    "E. coli": {
        "Overview": (
            "E. coli are germs called bacteria. They are found in many places, including in the environment, foods, "
            "water, and the intestines of people and animals. "
            "Most E. coli are harmless and are part of a healthy intestinal tract. E. coli help us digest food, "
            "produce vitamins, and protect us from harmful germs."
        ),
        "How it Spreads": "N/A",
        "Symptoms in Women": "But some E. coli can make people sick with diarrhea, urinary tract infections, pneumonia, sepsis, and other illnesses.",
        "Symptoms in Men": "But some E. coli can make people sick with diarrhea, urinary tract infections, pneumonia, sepsis, and other illnesses.",
        "Symptoms from Rectal Infections": "But some E. coli can make people sick with diarrhea, urinary tract infections, pneumonia, sepsis, and other illnesses.",
        "Prevention": "N/A",
        "Treatment and Recovery": "N/A"
    },
    "Giardia": {
        "Overview": (
            "Giardia duodenalis (Giardia for short) is a parasite. Illness caused by Giardia is called giardiasis. "
            "Giardia lives in the gut of infected people and animals and comes out of the body in poop. Giardia can survive for weeks to months outside the body (for example, in soil). "
            "In the United States, more people get sick from Giardia than from any other parasite that lives in the gut. Every year, more than 1 million people get sick from Giardia. "
            "Giardia is found in every region of the United States and around the world."
        ),
        "How it Spreads": (
            "You can get sick if you swallow Giardia. "
            "Giardia germs are in poop, so anything that gets contaminated by poop can potentially spread the germs. Giardia can spread from one person to another or through contaminated water, food, surfaces, or objects. "
            "Giardia spreads easily; swallowing just a few Giardia germs can make you sick."
        ),
        "Symptoms in Women": (
            "Symptoms usually begin by having diarrhea 2 to 5 times per day and feeling more and more tired. "
            "Short-term symptoms include: Diarrhea, Gas, Smelly, greasy poop that can float, Stomach cramps or pain, Upset stomach or nausea, Dehydration (loss of fluids). "
            "Symptoms usually begin 1 to 2 weeks after becoming infected with Giardia and last for 2 to 6 weeks. Occasionally, people have long-term symptoms that can last for years."
        ),
        "Symptoms in Men": (
            "Symptoms usually begin by having diarrhea 2 to 5 times per day and feeling more and more tired. "
            "Short-term symptoms include: Diarrhea, Gas, Smelly, greasy poop that can float, Stomach cramps or pain, Upset stomach or nausea, Dehydration (loss of fluids). "
            "Symptoms usually begin 1 to 2 weeks after becoming infected with Giardia and last for 2 to 6 weeks. Occasionally, people have long-term symptoms that can last for years."
        ),
        "Symptoms from Rectal Infections": (
            "Symptoms usually begin by having diarrhea 2 to 5 times per day and feeling more and more tired. "
            "Short-term symptoms include: Diarrhea, Gas, Smelly, greasy poop that can float, Stomach cramps or pain, Upset stomach or nausea, Dehydration (loss of fluids). "
            "Symptoms usually begin 1 to 2 weeks after becoming infected with Giardia and last for 2 to 6 weeks. Occasionally, people have long-term symptoms that can last for years."
        ),
        "Prevention": (
            "Wash your hands with soap and water at key times. "
            "Avoid swallowing water while swimming. "
            "Boil or filter water from lakes, springs, or rivers before drinking or preparing food with it. "
            "Wait to have sex for several weeks after you or your partner no longer have diarrhea. "
            "Avoid touching animal poop. "
            "Clean and disinfect areas where a person or pet recently had diarrhea."
        ),
        "Treatment and Recovery": (
            "Your healthcare provider may prescribe medicine to treat illness caused by Giardia. "
            "If you have diarrhea, drink a lot of water or other fluids to avoid dehydration (loss of fluids)."
        )
    },
    "Gonorrhea": {
        "Overview": (
            "Gonorrhea is an STI that can cause infection in the genitals, rectum, and throat. "
            "It is very common, especially among young people ages 15-24 years."
        ),
        "How it Spreads": (
            "You can get gonorrhea by having vaginal, anal, or oral sex without a condom with someone who has the infection. "
            "A pregnant person with gonorrhea can give the infection to their baby during childbirth."
        ),
        "Symptoms in Women": (
            "Most women with gonorrhea do not have any symptoms. Even when a woman has symptoms, they are often mild and can be mistaken for a bladder or vaginal infection. "
            "Symptoms in women can include: Painful or burning sensation when peeing, Increased vaginal discharge, Vaginal bleeding between periods."
        ),
        "Symptoms in Men": (
            "A burning sensation when peeing, A white, yellow, or green discharge from the penis, "
            "Painful or swollen testicles (although this is less common)."
        ),
        "Symptoms from Rectal Infections": (
            "Rectal infections may either cause no symptoms or cause symptoms in both men and women that may include: "
            "Discharge, Anal itching, Soreness, Bleeding, Painful bowel movements."
        ),
        "Prevention": (
            "The only way to completely avoid STIs is to not have vaginal, anal, or oral sex. "
            "If you are sexually active, the following things can lower your chances of getting gonorrhea: "
            "Being in a long-term mutually monogamous relationship with a partner who has been tested and does not have gonorrhea, "
            "Using condoms the right way every time you have sex."
        ),
        "Treatment and Recovery": (
            "Yes, the right treatment can cure gonorrhea. It is important that you take all of the medicine your healthcare provider gives you to cure your infection. "
            "Do not share medicine for gonorrhea with anyone. Although medicine will stop the infection, it will not undo any permanent damage caused by the disease. "
            "It is becoming harder to treat some gonorrhea, as drug-resistant strains of gonorrhea are increasing. "
            "Return to a healthcare provider if your symptoms continue for more than a few days after receiving treatment."
        )
    },
    "Salmonella": {
        "Overview": (
            "Salmonella are bacteria (germs) that can make people sick with an illness called salmonellosis. "
            "People can get infected after swallowing Salmonella. "
            "Salmonella live in the intestines of people and animals. People can get infected with Salmonella in many ways, including: "
            "Eating contaminated food, Drinking or having contact with contaminated water, Touching animals, animal poop, and the places animals live and roam."
        ),
        "How it Spreads": "N/A",
        "Symptoms in Women": "N/A",
        "Symptoms in Men": "N/A",
        "Symptoms from Rectal Infections": "N/A",
        "Prevention": "N/A",
        "Treatment and Recovery": "N/A"
    },
    "Shigella": {
        "Overview": (
            "Shigella bacteria cause an infection called shigellosis. Shigella cause an estimated 450,000 infections in the United States each year, "
            "and antimicrobial resistant infections result in an estimated $93 million in direct medical costs. "
            "The four species of Shigella are: Shigella sonnei (the most common species in the United States), Shigella flexneri, Shigella boydii, "
            "Shigella dysenteriae. S. dysenteriae and S. boydii are rare in the United States, though they continue to be important causes of disease "
            "in areas with less access to resources. Shigella dysenteriae type 1 can be deadly."
        ),
        "How it Spreads": (
            "Shigella spreads easily; swallowing just a small amount of Shigella germs can make you sick. "
            "Shigella germs are in poop, so anything that gets contaminated by poop can potentially spread the germs. "
            "Shigella can spread from one person to another or through contaminated water, food, surfaces, or objects."
        ),
        "Symptoms in Women": (
            "Symptoms usually start 1–2 days after infection and last 7 days. Most people with shigellosis experience: "
            "Diarrhea that can be bloody or prolonged (lasting more than 3 days), Fever, Stomach pain, "
            "Feeling the need to pass stool (poop) even when the bowels are empty."
        ),
        "Symptoms in Men": (
            "Symptoms usually start 1–2 days after infection and last 7 days. Most people with shigellosis experience: "
            "Diarrhea that can be bloody or prolonged (lasting more than 3 days), Fever, Stomach pain, "
            "Feeling the need to pass stool (poop) even when the bowels are empty."
        ),
        "Symptoms from Rectal Infections": (
            "Symptoms usually start 1–2 days after infection and last 7 days. Most people with shigellosis experience: "
            "Diarrhea that can be bloody or prolonged (lasting more than 3 days), Fever, Stomach pain, "
            "Feeling the need to pass stool (poop) even when the bowels are empty."
        ),
        "Prevention": (
            "Wash your hands with soap and water at key times. Take care when changing diapers. "
            "Avoid swallowing water while swimming. When traveling internationally, follow safe food and water habits and clean your hands often. "
            "If you or your partner has been diagnosed with shigellosis, do not have sex for at least two weeks after the diarrhea ends."
        ),
        "Treatment and Recovery": (
            "People who have shigellosis usually get better without antibiotic treatment in 5 to 7 days. "
            "People with mild shigellosis may need only fluids and rest. Your healthcare provider may prescribe medicine to treat illness caused by Shigella. "
            "If you have diarrhea, drink a lot of water or other fluids to avoid dehydration (loss of fluids)."
        )
    },
    "Valley Fever": {
        "Overview": (
            "Valley fever (coccidioidomycosis) is a lung infection caused by Coccidioides, a fungus that lives in the soil. "
            "The fungus is found in the Pacific Northwest and southwestern United States, and parts of Mexico, Central America, and South America. "
            "People can get Valley fever by breathing in the spores from Coccidioides. Some people breathe in the spores and never get sick. "
            "Valley fever causes typical lung infection symptoms like cough and fever. "
            "Certain groups of people are at increased risk for more severe illness. While it is very rare, the infection can spread to other parts of the body (called disseminated infection). "
            "Early treatment with antifungal medications can save lives. "
            "Because Valley fever has the same symptoms as pneumonias caused by bacteria or viruses, it is often misdiagnosed or undiagnosed. "
            "This can lead to ineffective treatments or delays in appropriate antifungal treatment for people who need it. "
            "Pets can also become infected. Valley fever does not spread from person to person or between people and animals."
        ),
        "How it Spreads": "N/A",
        "Symptoms in Women": (
            "Some people who are exposed to the fungus never have symptoms. Other people may have symptoms that include: "
            "Fatigue (tiredness), Cough, Fever and headache, Shortness of breath, Night sweats, Muscle aches or joint pain, Rash on upper body or legs."
        ),
        "Symptoms in Men": (
            "Some people who are exposed to the fungus never have symptoms. Other people may have symptoms that include: "
            "Fatigue (tiredness), Cough, Fever and headache, Shortness of breath, Night sweats, Muscle aches or joint pain, Rash on upper body or legs."
        ),
        "Symptoms from Rectal Infections": (
            "Some people who are exposed to the fungus never have symptoms. Other people may have symptoms that include: "
            "Fatigue (tiredness), Cough, Fever and headache, Shortness of breath, Night sweats, Muscle aches or joint pain, Rash on upper body or legs."
        ),
        "Prevention": "N/A",
        "Treatment and Recovery": "N/A"
    }
}

# Function to create the BigQuery table if it doesn't exist
def create_bigquery_table():
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define table schema
    schema = [
        bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Overview", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("How it Spreads", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Symptoms in Women", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Symptoms in Men", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Symptoms from Rectal Infections", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Prevention", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Treatment and Recovery", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.get_table(table_ref)  # Check if the table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        table = client.create_table(table)  # Create table if it doesn't exist
        print(f"Table {table_id} created successfully.")

# Function to store data in BigQuery
def store_data_in_bigquery(data):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    # Prepare rows for insertion
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

# HTTP Cloud Function
@functions_framework.http
def run_scraping_to_bigquery(request):
    try:
        # Create table if it doesn't exist
        create_bigquery_table()

        # Loop through each disease and store data
        for disease, info in predefined_data.items():
            info["Disease"] = disease  # Add disease name to the data dictionary
            store_data_in_bigquery(info)

        return "Data scraping and storing completed successfully.", 200
    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500
