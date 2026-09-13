#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd


def read_file_names(excel_path: Path, column: str) -> list[str]:
	dataframe = pd.read_excel(excel_path, sheet_name='total', usecols=[column])
	return [
		str(value).strip()
		for value in dataframe[column].dropna()
		if str(value).strip()
	]


def find_source_file(source_dir: Path, file_name: str) -> Path | None:
	requested_path = Path(file_name)
	direct_match = source_dir / requested_path
	if direct_match.is_file():
		return direct_match

	if requested_path.suffix:
		matches = [
			match
			for match in source_dir.rglob(requested_path.name)
			if match.is_file()
		]
	else:
		requested_stem = requested_path.name.casefold()
		matches = [
			match
			for match in source_dir.rglob("*")
			if match.is_file() and match.stem.casefold() == requested_stem
		]
	if len(matches) > 1:
		print(f"Skipped ambiguous file name: {file_name} ({len(matches)} matches)")
		return None
	return matches[0] if matches else None


def copy_files(
	file_names: list[str],
	source_dir: Path,
	destination_dir: Path,
	overwrite: bool,
	dry_run: bool,
) -> tuple[int, int]:
	completed = 0
	missing = 0

	for file_name in file_names:
		source_file = find_source_file(source_dir, file_name)
		if source_file is None:
			print(f"Not found: {file_name}")
			missing += 1
			continue

		destination_file = destination_dir / source_file.relative_to(source_dir)
		if destination_file.exists() and not overwrite:
			print(f"Skipped existing file: {destination_file}")
			continue

		print(f"Copy: {source_file} -> {destination_file}")
		if not dry_run:
			destination_file.parent.mkdir(parents=True, exist_ok=True)
			shutil.copy2(source_file, destination_file)
		completed += 1

	return completed, missing


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Copy files named in an Excel column."
	)
	parser.add_argument("--excel", required=True, help="Excel workbook containing file names")
	parser.add_argument("--source-dir", required=True, help="Directory containing source files")
	parser.add_argument("--destination-dir", required=True, help="Directory to receive files")
	parser.add_argument(
		"--column",
		default="filename",
		help="Excel column containing file names (default: filename)",
	)
	parser.add_argument("--overwrite", action="store_true", help="Replace existing files")
	parser.add_argument("--dry-run", action="store_true", help="Print planned operations only")
	args = parser.parse_args()

	excel_path = Path(args.excel).expanduser().resolve()
	source_dir = Path(args.source_dir).expanduser().resolve()
	destination_dir = Path(args.destination_dir).expanduser().resolve()

	if not excel_path.is_file():
		raise FileNotFoundError(f"Excel file does not exist: {excel_path}")
	if not source_dir.is_dir():
		raise NotADirectoryError(f"Source directory does not exist: {source_dir}")
    
	file_names = read_file_names(excel_path, args.column)
	completed, missing = copy_files(
		file_names=file_names,
		source_dir=source_dir,
		destination_dir=destination_dir,
		overwrite=args.overwrite,
		dry_run=args.dry_run,
	)
	print(f"Processed {completed} file(s); {missing} file(s) were not found.")


if __name__ == "__main__":
	main()
