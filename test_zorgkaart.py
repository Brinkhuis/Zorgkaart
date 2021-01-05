import zorgkaart

def test_get_types():
    df =  zorgkaart.get_types()
    assert df.shape[0] > 0, "Should be greater than 0)"

def test_get_info():
    df =  zorgkaart.get_types().sort_values('aantal').reset_index()
    row = df.iloc[5] # pick row 5 as test case
    assert zorgkaart.get_info(row.organisatietype).shape[0] == row.aantal, f"Should be {row.aantal}"

def test_get_details():
    df = zorgkaart.get_types().sort_values('aantal').reset_index()
    row = df.iloc[5] # pick row 5 as test case
    assert zorgkaart.get_details(row.organisatietype).shape[0] == row.aantal, f"Should be {row.aantal}"
