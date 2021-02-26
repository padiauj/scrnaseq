import argparse
import glob
import logging
import subprocess
from datetime import datetime

logger = logging.getLogger("run")

DEFAULT_DRY = False


def get_file_order(lst):
    final = []
    for fastq in lst:
        if ("_R1_" in fastq):
            r2 = fastq.replace("_R1_", "_R2_")
            if (r2 in lst):
                final.append(fastq)
                final.append(r2)
    return final


def run_cmd(params, dry=DEFAULT_DRY):
    if (not dry):
        proc = subprocess.run(params, capture_output=True)
        logger.info(proc.stdout.decode('utf-8'))
        if (proc.stderr):
            logger.error(proc.stderr.decode('utf-8'))


def init_logger():
    timestr = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    fname = "output_{time}.log".format(time=timestr, encoding='utf-8')
    logging.basicConfig(filename=fname,
                        level=logging.DEBUG,
                        format='%(asctime)s:%(levelname)s: %(message)s')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    return logger, fname


def name_str(name):
    keepcharacters = (' ', '.', '_')
    safe_name = "".join(c for c in name.strip()
                        if c.isalnum() or c in keepcharacters).rstrip()
    return safe_name


def seq_link(link):
    return link


if __name__ == "__main__":
    logger, logpath = init_logger()
    parser = argparse.ArgumentParser(description="Deploy scrna analysis tasks")
    parser.add_argument('name',
                        metavar='name',
                        type=name_str,
                        help="Sample name to uniquely identify run")
    parser.add_argument(
        'link',
        metavar='link',
        type=seq_link,
        help=
        "Names and links to BAM files to process e.g. sample1,http://example.org/sample1.bam sample2,http://example.org/sample2.bam"
    )
    parser.add_argument('--dry', dest='dry', action='store_true')
    parser.set_defaults(dry=False)

    args = parser.parse_args()
    nthreads = 16

    outbam = "{acc}.bam".format(acc=args.name)
    fastqpath = "fastqs/{acc}".format(acc=args.name)

    # Fetch data
    logger.info("Downloading {link} to {outbam}".format(link=args.link,
                                                        outbam=outbam))
    aria_call = ["aria2c", "-x", str(nthreads), "-o", outbam, args.link]
    logger.info(" ".join(aria_call))
    run_cmd(aria_call, dry=args.dry)

    # BAM to FASTQ Conversion
    logger.info("Converting {outbam} to fastqs at path {outpath}".format(
        outbam=outbam, outpath=fastqpath))
    bamfastq_call = [
        "./bamtofastq-1.3.2",
        "--nthreads={nthreads}".format(nthreads=nthreads), outbam, fastqpath
    ]
    logger.info(" ".join(bamfastq_call))
    run_cmd(bamfastq_call, dry=args.dry)
    logger.info("Removing BAM...")
    run_cmd(["rm", "-rf", outbam], dry=args.dry)

    # Alignment
    logger.info("Running Kallisto on {fastqpath}".format(fastqpath=fastqpath))
    files = get_file_order(
        sorted(glob.glob(fastqpath + "/gemgroup001/*_R*.fastq.gz")))
    outpath = "out_{acc}".format(acc=args.name)
    kb_call = [
        "kb", "count", "-i", "kb_human.idx", "-g", "kbtg.txt", "-x", "10xv2",
        "--h5ad", "--cellranger"
    ] + files + ["-t", str(nthreads), "-o", outpath]
    logger.info(" ".join(kb_call))
    run_cmd(kb_call, dry=args.dry)

    logger.info("Sync results with S3")
    aws_sync_results = [
        "aws",
        "s3",
        "sync",
        outpath,
        "s3://umesh-churchlab/{path}".format(path=outpath),
    ]
    logger.info(" ".join(aws_sync_results))
    run_cmd(aws_sync_results, dry=args.dry)
    aws_sync_log = [
        "aws",
        "s3",
        "cp",
        logpath,
        "s3://umesh-churchlab/{path}".format(path=logpath),
    ]
    logger.info(" ".join(aws_sync_log))

    run_cmd(aws_sync_log, dry=args.dry)

    logger.info("Cleaning up... ")
    run_cmd(["rm", "-rf", fastqpath], dry=args.dry)
    run_cmd(["rm", "-rf", outpath], dry=args.dry)
