import glob
import logging
import subprocess
import time

DRY = True

timestr = time.strftime("%Y%m%d-%H%M%S")

logging.basicConfig(filename="output-{time}.log".format(
    time=timestr, encoding='utf-8', level=logging.DEBUG))


def run_cmd(params, dry=False):
    if (not dry):
        subprocess.call(params)


if __name__ == "__main__":
    links = {}
    with open("links.txt", 'r') as f:
        for line in f:
            links[line.split()[0]] = line.split()[1]

    logging.debug("(name, link) pairs to process: " + str([
        "({name}, {link})".format(name=name, link=link)
        for name, link in links.items()
    ]))
    logging.info("Found {links} links to process.".format(links=len(links)))

    for acc in links:
        outbam = "{acc}.bam".format(acc=acc)
        fastqpath = "fastqs/{acc}".format(acc=acc)

        # Fetch data
        logging.info("Downloading {link} to {outbam}".format(link=links[acc]))
        aria_call = ["aria2c", "-x", "16", "-o", outbam, links[acc]]
        logging.info(" ".join(aria_call))
        run_cmd(aria_call, dry=DRY)

        # BAM to FASTQ Conversion
        logging.info("Converting {outbam} to fastqs at path {outpath}".format(
            outbam=outbam, outpath=fastqpath))
        bamfastq_call = [
            "./bamtofastq-1.3.2", "--nthreads=16", outbam, fastqpath
        ]
        logging.info(" ".join(bamfastq_call))
        run_cmd(bamfastq_call, dry=DRY)
        run_cmd(["rm", "-rf", outbam], dry=DRY)

        # Alignment
        logging.info(
            "Running Kallisto on {fastqpath}".format(fastqpath=fastqpath))
        print(glob.glob(fastqpath + "/gemgroup001/*_R*.fastq"))
        files = sorted(glob.glob(fastqpath + "/gemgroup001/*_R*.fastq.gz"))
        outpath = "out_{acc}".format()
        kb_call = [
            "kb", "count", "-i", "kallisto/kb_human.idx", "-g",
            "kallisto/kbtg.txt", "-x", "10xv2", "--h5ad", "--report"
        ] + files + ["-t", "16", "-o", "kallisto/{acc}".format(acc=acc)]
        logging.info(" ".join(kb_call))
        run_cmd(kb_call, dry=DRY)

        logging.info("Sync results with S3")
        run_cmd([
            "aws", "s3", "sync", outpath,
            "s3://umesh-churchlab/{path}".format(path=outpath)
        ])

        logging.info("Cleaning up... ")
        run_cmd(["rm", "-rf", fastqpath], dry=DRY)
