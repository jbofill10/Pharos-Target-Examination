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

        # Ligand Activity
        query(connection, table='ncats_ligand_activity', columns=['id', 'ncats_ligand_id', 'target_id', 'smiles', 'act_value', 'act_type', 'action_type', 'reference', 'reference_source', 'pubmed_ids'],
              save_dest='Data/ligands')

        # Ortholog Data
        query(connection, table='ortholog', columns=['id', 'protein_id', 'taxid', 'species', 'db_id', 'geneid', 'symbol', 'name', 'mod_url', 'sources'], save_dest='Data/ortholog')

        # Go annotations
        query(connection, table='goa', columns=['id', 'protein_id', 'go_id', 'go_term', 'evidence', 'goeco', 'assigned_by'], save_dest='Data/goa')
        
        # Ab count
        query(connection, table='tdl_info', columns=['id', 'itype', 'target_id', 'protein_id', 'nucleic_acid_id', 'string_value', 'number_value', 'integer_value', 'date_value', 'boolean_value', 'curration_level'], 
            save_dest='Data/tdl_info',
            condition=" where itype = 'Ab Count'")

    # OMIM Publications

        query(connection, table='disease', columns=['id', 'dtype', 'protein_id', 'nhprotein_id', 'name', 'ncats_name', 'did', 
            'evidence', 'zscore', 'conf', 'description', 'reference', 'drug_name', 'log2foldchange', 'pvalue', 'score', 'source', 'OS2', 'S20'], save_dest='Data/omim', condition=" where source = 'OMIM';")

    # HGram

        query(connection, table='hgram_cdf', columns=['id', 'protein_id', 'type', 'attr_count', 'attr_cdf'], save_dest='Data/hgram')
        
    except Exception as error:
        raise error


def query(connection, table, columns, save_dest, condition=''):
    cursor = connection.cursor()

    try:
        cursor.execute(f'select * from {table}{condition};')
        res = cursor.fetchall()

        res = [list(i) for i in res]

        df = pd.DataFrame(res, columns=columns)

        df.to_pickle(save_dest)
        print(f'Query to {table} successful')
    except Exception as error:
        print(f"Failed fetching {table} Data\n\n")
        raise error


if __name__ == '__main__':
    run()