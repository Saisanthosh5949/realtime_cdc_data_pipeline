from pathlib import Path
from src.common.spark_session import get_spark_session
def show(spark,path,title):
    print("\n"+title)
    if not Path(path).exists():
        print("No output yet"); return
    df=spark.read.parquet(path)
    print("records:",df.count()); df.show(20,truncate=False)
def main():
    spark=get_spark_session("InspectCDC")
    show(spark,"data/silver/customer_changes","CDC CHANGE EVENTS")
    show(spark,"data/gold/customer_current","CURRENT CUSTOMER STATE")
    show(spark,"data/gold/customer_history","CUSTOMER HISTORY")
    show(spark,"data/bad_records","BAD RECORDS")
    spark.stop()
if __name__=="__main__": main()
