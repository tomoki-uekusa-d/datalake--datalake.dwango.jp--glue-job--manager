import argparse
import subprocess
from datetime import datetime, timedelta

TARGET_JOB_LIST = [
    # ("melody_android", "latest"),
    # ("melody_android", "amuse"),
    # ("dwango_jp_ios", "niconico"),
    # ("dwango_jp_android", "niconico"),
    ("animelomix_ios", "wallpaper"),
    ("animelomix_ios", "voice"),
    ("animelomix_android", "wallpaper"),
    ("animelomix_android", "voice"),
]

parser = argparse.ArgumentParser()

parser.add_argument('-b', '--bucket', help='bucket', type=str, default="etl-datadomain-new-arrivals")
parser.add_argument('-s', '--site', help='site', type=str)
parser.add_argument('-c', '--corner', help='corner', type=str)
parser.add_argument('-st', '--startdate', help='startdate', type=str)
parser.add_argument('-n', '--number', help='number', type=int, default=7)

args = parser.parse_args()

if (not args.site and args.corner) and (args.site and not args.corner):
    raise Exception("'site' and 'corner' should be setted both or neither")

is_use_target_job_list = True if (not args.site and not args.corner) else False

if not args.startdate:
    str_stdt = datetime.now()
else:
    str_stdt = datetime.strptime(args.startdate, "%Y%m%d")

datelist = []
for i in range(args.number):
    datelist.append(str_stdt + timedelta(days=i))

if is_use_target_job_list:
    for site, corner in TARGET_JOB_LIST:
        print(f"=========== {site}_{corner} ==========")
        for d in datelist:
            d_str = d.strftime("%Y%m%d")
            # print(f"aws s3 cp s3://{args.bucket}/fact_new_arrivals/site={site}/corner={corner}/target_date={d_str}/{d_str}_{site}_{corner}.csv - | wc -l")
            cmd = f"aws s3 cp s3://{args.bucket}/fact_new_arrivals/site={site}/corner={corner}/target_date={d_str}/{d_str}_{site}_{corner}.csv - | wc -l"
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            row_number = int(proc.communicate()[0].decode("utf-8"))
            if row_number > 0:
                print(f"Found {d_str} {site}_{corner} {row_number}")
else:
    for d in datelist:
        d_str = d.strftime("%Y%m%d")
        # print(f"aws s3 cp s3://{args.bucket}/fact_new_arrivals/site={args.site}/corner={args.corner}/target_date={d_str}/{d_str}_{args.site}_{args.corner}.csv -")
        cmd = f"aws s3 cp s3://{args.bucket}/fact_new_arrivals/site={args.site}/corner={args.corner}/target_date={d_str}/{d_str}_{args.site}_{args.corner}.csv -"
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        row_number = int(proc.communicate()[0].decode("utf-8"))
        if row_number > 0:
            print(f"Found {d_str} {site}_{corner} {row_number}")
