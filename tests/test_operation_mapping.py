def map_op(op):
    return {"c":"INSERT","u":"UPDATE","d":"DELETE","r":"SNAPSHOT"}.get(op,"UNKNOWN")
def test_cdc_operation_mapping():
    assert map_op("c")=="INSERT"
    assert map_op("u")=="UPDATE"
    assert map_op("d")=="DELETE"
    assert map_op("r")=="SNAPSHOT"
