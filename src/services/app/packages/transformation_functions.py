"""
    These are the functions that are being used in the etl pipeline
"""
import random
import pandas as pd


def create_alp() -> str:
    """ This functions is beings used for the creation of fake soundex entries
    Returns:
        str: a Soundex entrie e.g. N310, B902 ...
    """
    return str(chr(random.randrange(65, 90))) + str(chr(random.randrange(48, 54))) + str(
        chr(random.randrange(48, 54))) + str(chr(random.randrange(48, 54)))


def create_noise(noise: int, size: int, name: str) -> pd.Series:
    """ This function is being used to add noise in our dataset. It requires the percentage of the noise (e.g. 10%, 20%)
        the size of the dataset (e.g 10.000 entries) and the name of the column.
    Args:
        noise (int): The percentage of the noise we are going to use in our dataset (e.g. 10%, 20%)
        size (int):  The size of the dataset (e.g. 25.000, 50.000)
        name (str):  The name of the column
    Returns:
        pd.Series: A series with the soundex values values
    """
    return pd.Series([create_alp() for _ in range(int(noise * size / 100))], dtype='string', name=name)


def create_fake_index(noise: int, size: int, name: str) -> pd.Series:
    """Creates a column field with the fake index as a value
    Args:
        noise (int): The percentage of the noise we are going to use in our dataset (e.g. 10%, 20%)
        size (int):  The size of the dataset (e.g. 25.000, 50.000)
        name (str):  The name of the column
    Returns:
        pd.Series: _description_
    """
    return pd.Series(['Fake Index' for _ in range(int(noise * size / 100))], dtype='string', name=name)


def create_fake_soundex_values(noise: int, dataframe: pd.DataFrame, m_field: str) -> pd.DataFrame:
    """Creates a dataframe with a Soundex values which will be merged with our dataset in order too add layers
       of noise. This will apply extra protection to our data.
    Args:
        noise (int): The percentage of the noise we are going to use in our dataset (e.g. 10%, 20%)
        dataframe (pd.DataFrame): The dataframe with the data
        m_field (str): the column which will be used to make the join operation to find our matches
    Returns:
        pd.DataFrame: The dataset with the fake soundex values
    """
    size = dataframe.shape[0]

    fake_index = create_fake_index(noise, size, m_field)
    df = pd.DataFrame(fake_index, columns=[m_field])

    for row in dataframe.columns:
        if row != m_field:
            noise_df = pd.Series(create_noise(noise, size, row), name=row)
            df = pd.concat([df, noise_df], axis=1)

    return df