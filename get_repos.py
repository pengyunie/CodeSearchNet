from pathlib import Path
from typing import Union

from jsonargparse import CLI
from jsonargparse.typing import Path_drw, Path_dc
import seutil as su
from tqdm import tqdm


def get_repos(
    language: str,
    download_dir: Union[Path_drw, Path_dc, Path] = Path.cwd() / "resources" / "data",
    out_dir: Union[Path_drw, Path_dc, Path] = Path.cwd() / "repos",
):
    if not isinstance(download_dir, Path):
        download_dir = Path(download_dir.abs_path)
    if not isinstance(out_dir, Path):
        out_dir = Path(out_dir.abs_path)

    with su.io.cd(download_dir):
        data_dir = download_dir / language
        # download the zip file and unzip, if not already
        if not data_dir.exists():
            su.bash.run(
                f"wget https://s3.amazonaws.com/code-search-net/CodeSearchNet/v2/{language}.zip -O {language}.zip",
                0,
            )
            su.bash.run(f"unzip {language}.zip", 0)
            with su.io.cd(data_dir):
                su.bash.run("find -name '*.gz' | xargs gzip -d", 0)
            su.io.rm(f"{language}.zip")

    seen_repos = set()
    for sn in ["train", "valid", "test"]:
        files = sorted((data_dir / "final" / "jsonl" / sn).glob("*.jsonl"))
        dataset = []
        for f in tqdm(files, desc=sn):
            dataset += su.io.load(f)

        print(f"{len(dataset)} items in {sn}")
        repos = set([d["repo"] for d in dataset])
        print(f"{len(repos)} repos in {sn}")

        overlap_repos = seen_repos & repos
        print(f"{len(overlap_repos)} repos in {sn} overlap with previous")
        seen_repos |= repos

        su.io.dump(
            out_dir / language / f"{sn}_repos.txt", sorted(repos), su.io.Fmt.txtList
        )


if __name__ == "__main__":
    CLI(get_repos, as_positional=False)
