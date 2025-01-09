import pandas as pd

def updateDimensionTable(engine, table, data, pk="id"):
    """
    Author: Maximiliano Fernandez

    This function updates a dimension table in a data warehouse with new data.
    If the data already exists in the table, it is not added.
    It returns the updated table with the primary keys as they are in the database.

    The dimension table must be created and the columns must have the same names as in the dataframe.

    Parameters:
        engine (sqlalchemy.engine.Engine): Database engine.
        table (str): The name of the dimension table to update.
        data (pandas.DataFrame): Dataframe of new data to be added, excluding the primary key
        pk (str, optional): Name of the primary key. Default is "id"

    Returns:
        dimension_df: The updated dimension table as a DataFrame.
    """
    with engine.connect() as conn, conn.begin():
        old_data = pd.read_sql_table(table, conn)

        # Delete the pk column
        old_data.drop(pk, axis=1, inplace=True)

        # 'new_data' is the dataframe of set difference between 'old_data'
        new_data = data[~data.stack().isin(old_data.stack().values).unstack()].dropna()

        # Insert 'new_data'
        new_data.to_sql(table, conn, if_exists='append', index=False)

        # Query and return the final data
        dimension_df = pd.read_sql_table(table, conn)

    return dimension_df


def updateDimensionTableIntPK(engine, table, data, pk="id"):
    """
    Update a dimension table in a database using the provided engine, table name, data, and primary key.
    This function is used when the primary key is an integer and not a serial.

    Parameters:
        engine (sqlalchemy.engine.Engine): Database engine
        table (str): The name of the dimension table to update.
        data (pandas.DataFrame): Dataframe of new data to be added, excluding the primary key
        pk (str, optional): Name of the primary key. Default is "id"

    Returns:
        pandas.DataFrame: The updated dimension table as a DataFrame.
    """
    with engine.connect() as conn, conn.begin():
        existing_data = pd.read_sql_table(table, conn)

        for index, row in data.iterrows():
            pk_value = row[pk]
            existing_index = existing_data[existing_data[pk] == pk_value].index
            if len(existing_index) > 0:
                pass
            else:
                row.to_frame().T.to_sql(table, conn, if_exists='append', index=False)

        # Query and return the final data
        dimension_df = pd.read_sql_table(table, conn)

    return dimension_df