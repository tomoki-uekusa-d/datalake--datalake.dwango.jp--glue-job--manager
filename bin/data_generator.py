from datetime import datetime
from dateutil.relativedelta import relativedelta
import argparse
import logging
import subprocess
import time

LOG_LEVEL = logging.DEBUG

logger = logging.getLogger("data_generator")
logger.setLevel(LOG_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
formatter = logging.Formatter("%(asctime)s %(name)s:%(lineno)s %(funcName)s [%(levelname)s]: %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class DataGenerator():

    def __init__(
        self,
        job_name,
        start_month,
        end_month,
        site,
        corner,
        bucket,
        skip_exists,
        dryrun,
        skip_months,
        target_row_count=101, # including header line
        wait_seconds=300,
    ) -> None:
        self.job_name = job_name
        self.start_month = start_month
        self.end_month = end_month
        self.target_row_count = target_row_count
        self.site = site
        self.corner = corner
        self.bucket = bucket
        self.wait_seconds = int(wait_seconds)
        self.skip_exists = skip_exists
        self.dryrun = dryrun
        self.skip_months = skip_months
        self.executioning_month = start_month if start_month else datetime.now().strftime("%Y%m")
        self.scheduled_months = []
        self.executioned_months = [self.start_month]
        self.limit_array_length = 100

    
    def execute_job(self):
        arg_target_month = datetime.strptime(self.executioning_month, "%Y%m").strftime("%Y-%m")
        cmd=f"aws glue start-job-run --job-name {self.job_name} --arguments='--TARGET_MONTH=\"{arg_target_month}\"' | grep JobRunId | awk '{{print $NF}}' | sed -e 's/\"//g'"
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout = proc.communicate()
        run_id = str(stdout[0].decode("utf-8")).strip()
        if not run_id or run_id == "":
            logger.error(f"RUN_ID: {run_id}, STDOUT: {stdout}")
            raise Exception("run_id is NOT Nullable")
        logger.info(f"Executed job, RUN_ID: {run_id}, URL: https://ap-northeast-1.console.aws.amazon.com/gluestudio/home?region=ap-northeast-1#/job/{self.job_name}/run/{run_id}")

        while not self.is_completed_job(run_id):
            logger.info(f"Waiting job, STATUS: {self.get_job_status(run_id)}")
            time.sleep(self.wait_seconds)

        logger.info(f"Finished job, STATUS: {self.get_job_status(run_id)}")


    def is_completed_job(self, run_id):
        return not self.get_job_status(run_id) in ["WAITING", "STARTING", "RUNNING"]


    def get_job_status(self, run_id):
        cmd=f"aws glue get-job-run --job-name {self.job_name} --run-id {run_id} | grep JobRunState | awk '{{print $NF}}' | sed -e 's/\"//g' | sed -e 's/,//g'"
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout = proc.communicate()
        status = str(stdout[0].decode("utf-8")).strip()
        if not status in [
            "ERROR",
            "FAILED",
            "RUNNING",
            "STARTING",
            "STOPPED",
            "STOPPING",
            "SUCCEEDED",
            "TIMEOUT",
            "WAITING",
        ]:
            logger.error(f"Invalid job status, STATUS: {status}, STDOUT: {stdout}")
            raise Exception("Glue job status is invalid.")
        return status


    def get_total_rows(self):
        total_rows = 0
        for target_month in self.scheduled_months:
            row_number = self.get_rows(target_month)
            total_rows += row_number
        logger.info(f"Total Row number: {total_rows}")
        return total_rows


    def get_rows(self, target_month):
        target_date = f"{target_month}01"
        cmd = f"aws s3 cp s3://{self.bucket}/fact_new_arrivals/site={self.site}/corner={self.corner}/target_date={target_date}/{target_date}_{self.site}_{self.corner}.csv - | wc -l"
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        row_number = int(proc.communicate()[0].decode("utf-8").strip())
        logger.info(f"Row number: {row_number}, File path: s3://{self.bucket}/fact_new_arrivals/site={self.site}/corner={self.corner}/target_date={target_date}/{target_date}_{self.site}_{self.corner}.csv")
        return row_number


    def is_complete(self):
        return self.get_total_rows() >= self.target_row_count or self.executioning_month == self.backward_month(self.end_month)


    def backward_month(self, from_month):
        to_month = datetime.strftime(datetime.strptime(f"{from_month}01", "%Y%m%d") - relativedelta(months=1), "%Y%m")
        logger.info(f"Backward 1 month, FROM:{from_month} , TO: {to_month}")
        return to_month


    def is_skip(self):
        if self.dryrun:
            return True
        if self.skip_exists:
            if self.executioning_month in self.skip_months.split(","):
                return True
        else:
            return False


    def generate_month_array(self, start_month, end_month):
        generated_array = []
        execution_month = start_month
        while True:
            generated_array.append(execution_month)
            execution_month = self.backward_month(execution_month)
            if execution_month == end_month or len(generated_array) >= self.limit_array_length:
                break
        return generated_array


    def main(self):
        logger.info(f"Start generating target data, JOB_NAME: {self.job_name}, TARGET_ROW_COUNT: {self.target_row_count}")
        self.scheduled_months = self.generate_month_array(start_month=self.start_month, end_month=self.end_month)
        while not self.is_complete():
            logger.info(f"Checking monthly data, EXECUTIONING_MONTH: {self.executioning_month}")
            if not self.is_skip():
                self.execute_job()
            else:
                logger.info(f"Skipped executing month, EXECUTIONING_MONTH: {self.executioning_month}")
            self.executioning_month = self.backward_month(self.executioning_month)
            self.executioned_months.append(self.executioning_month)

        logger.info("Finished, target data is generated!!")

        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-jn', '--job_name', help='job name', type=str, required=True)
    parser.add_argument('-sm', '--start_month', help='start month YYYYMM', type=str, required=True)
    parser.add_argument('-em', '--end_month', help='end month YYYYMM', type=str, required=True)
    parser.add_argument('-b', '--bucket', help='bucket', type=str, default="etl-datadomain-new-arrivals")
    parser.add_argument('-s', '--site', help='site', type=str)
    parser.add_argument('-c', '--corner', help='corner', type=str)
    parser.add_argument('-se', '--skip_exists', help='skip exists', action="store_true", default=False)
    parser.add_argument('-dr', '--dryrun', help='dryrun', action="store_true", default=False)
    parser.add_argument('-sms', '--skip_months', help='skipped months e.g. "202209,202208"', type=str, default="")
    args = parser.parse_args()

    dg = DataGenerator(
        job_name=args.job_name,
        start_month=args.start_month,
        end_month=args.end_month,
        bucket=args.bucket,
        site=args.site,
        corner=args.corner,
        skip_exists=args.skip_exists,
        dryrun=args.dryrun,
        skip_months=args.skip_months,
    )
    dg.main()
