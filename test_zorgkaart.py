import pandas as pd

import zorgkaart

def test_get_types():
    df =  zorgkaart.get_types()
    assert df.shape[0] > 0, "Should be greater than 0)"

