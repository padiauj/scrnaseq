import argparse
import glob
import logging
import subprocess
from datetime import datetime

logger = logging.getLogger("run")

DRY = False


def run_cmd(params, dry=DRY):
    if (not dry):
        proc = subprocess.run(params, capture_output=True)
        logger.info(proc.stdout.decode('utf-8'))
        if (proc.stderr):
            logger.error(proc.stderr.decode('utf-8'))


def init_logger():
    timestr = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    formatter = logging.Formatter()
    logging.basicConfig(filename="output_{time}.log".format(
        time=timestr, encoding='utf-8', level=logging.DEBUG),
                        format='%(asctime)s:%(levelname)s: %(message)s')
    logger.setLevel(logging.DEBUG)
    return logger


if __name__ == "__main__":
    logger = init_logger()
    parser = argparse.ArgumentParser(description="Deploy scrna analysis tasks")
    parser.add_argument(
        'pairs',
        metavar='name,link',
        type=str,
        nargs='+',
        help=
        "Names and links to BAM files to process e.g. sample1,http://example.org/sample1.bam sample2,http://example.org/sample2.bam"
    )
    args = parser.parse_args()

    # TODO check inputs
    links = {
        pair.split(",")[0].strip(): pair.split(",")[1].strip()
        for pair in args.pairs
    }

    logger.debug("(name, link) pairs to process: " + str([
        "({name}, {link})".format(name=name, link=link)
        for name, link in links.items()
    ]))
    logger.info("Found {links} links to process.".format(links=len(links)))

    for acc in links:
        outbam = "{acc}.bam".format(acc=acc)
        fastqpath = "fastqs/{acc}".format(acc=acc)

        # Fetch data
        logger.info("Downloading {link} to {outbam}".format(link=links[acc],
                                                            outbam=outbam))
        aria_call = ["aria2c", "-x", "16", "-o", outbam, links[acc]]
        logger.info(" ".join(aria_call))
        run_cmd(aria_call)

        # BAM to FASTQ Conversion
        logger.info("Converting {outbam} to fastqs at path {outpath}".format(
            outbam=outbam, outpath=fastqpath))
        bamfastq_call = [
            "./bamtofastq-1.3.2", "--nthreads=16", outbam, fastqpath
        ]
        logger.info(" ".join(bamfastq_call))
        run_cmd(bamfastq_call)
        run_cmd(["rm", "-rf", outbam])

        # Alignment
        logger.info(
            "Running Kallisto on {fastqpath}".format(fastqpath=fastqpath))
        files = sorted(glob.glob(fastqpath + "/gemgroup001/*_R*.fastq.gz"))
        outpath = "out_{acc}".format(acc=acc)
        kb_call = [
            "kb", "count", "-i", "kallisto/kb_human.idx", "-g",
            "kallisto/kbtg.txt", "-x", "10xv2", "--h5ad", "--report"
        ] + files + ["-t", "16", "-o", "kallisto/{acc}".format(acc=acc)]
        logger.info(" ".join(kb_call))
        run_cmd(kb_call, dry=DRY)

        logger.info("Sync results with S3")
        run_cmd([
            "aws", "s3", "sync", outpath,
            "s3://umesh-churchlab/{path}".format(path=outpath)
        ])

        logger.info("Cleaning up... ")
        run_cmd(["rm", "-rf", fastqpath])
