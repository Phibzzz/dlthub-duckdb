import dlt
from dlt.sources.rest_api import rest_api_source

def lower_email(record):
    record["email"] = record["email"].lower()
    return record


# Step 1 - Configuration de la source REST API - sans filtrage et transformation
source = rest_api_source({
    "client": {
        "base_url": "https://jsonplaceholder.typicode.com/"
    },
    "resources": [
        {"name": "users",
        "processing_steps": [
            {"filter": lambda x: x['id'] % 2 != 0},
            {"map": lower_email}
        ]
        },
        "posts"
    ]
})


# Définition du pipeline 
pipeline = dlt.pipeline(
    pipeline_name="jsonplaceholder_test",
    destination="duckdb"
)


if __name__ == "__main__":
    pipeline.run(source, write_disposition="replace")    

    with pipeline.sql_client() as client:
        try:
            #Ajout manuel de la clé primaire
            client.execute("ALTER TABLE jsonplaceholder_test_dataset.users ADD CONSTRAINT users_pk PRIMARY KEY (id);")
            print("Contrainte Primary Key ajoutée avec succès sur users.id")
        except Exception as e:
            if "already exists" in str(e):
                print("Contrainte Primary Key déjà existante sur users.id")
            else:
                raise # Renvoyer l'exception si ce n'est pas une contrainte déjà existante

        try:    
            client.execute("ALTER TABLE jsonplaceholder_test_dataset.posts ADD CONSTRAINT posts_pk PRIMARY KEY (id);")
            print("Contrainte Primary Key ajoutée avec succès sur posts.id")
        except Exception as e:
            if "already exists" in str(e):
                print("Contrainte Primary Key déjà existante sur posts.id")
            else:
                raise # Renvoyer l'exception si ce n'est pas une contrainte déjà existante
