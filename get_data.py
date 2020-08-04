import mysql.connector
import pandas as pd

def run():

    try:
        connection = mysql.connector.connect(
            host='tcrd.kmc.io',
            user='tcrd',
            database='tcrd660',
            password=''
        )
        print('Connected to TCRD Database')

        # Target Data
        query(connection, table='target', columns=['id', 'name', 'ttype', 'description', 'comment', 'tdl', 'idg', 'fam','famext'],
              save_dest='Data/pharos_data')

        # PM Scores
        query(connection, table='pmscore', columns=['id', 'protein_id', 'year', 'score'], save_dest='Data/pm_scores')

        # Gene RIFs
        query(connection, table='generif', columns=['id', 'protein_id', 'pubmed_ids', 'text','years'], save_dest='Data/generif')

        # PubTator Scores
        query(connection, table='ptscore', columns=['id', 'protein_id', 'year', 'score'], save_dest='Data/pt_scores')

        # PubMed Stuff
        query(connection, table='ncats_ligands', columns=['id', 'identifier', 'name', 'isDrug', 'smiles', 'PubChem', 'ChEMBL', 'Guide to Pharmacology', 'DrugCentral', 'description', 'actCnt'],
              save_dest='Data/ligands')

        # Ortholog Data
        query(connection, table='ortholog', columns=['id', 'protein_id', 'taxid', 'species', 'db_id', 'geneid', 'symbol', 'name', 'mod_url', 'sources'], save_dest='Data/ortholog')


    except Exception as error:
        raise error


def query(connection, table, columns, save_dest):
    cursor = connection.cursor()

    try:
        cursor.execute(f'select * from {table};')
        res = cursor.fetchall()

        res = [list(i) for i in res]

        df = pd.DataFrame(res, columns=columns)

        df.to_pickle(save_dest)
        print('Query successful')
    except Exception as error:
        print("Failed fetching Target Data\n\n")
        raise error


if __name__ == '__main__':
    run()