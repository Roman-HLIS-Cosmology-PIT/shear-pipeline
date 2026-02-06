import os
import time
from pathlib import Path

# import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
# from pyarrow import acero

from scm_pipeline import PipelineStage
from scm_pipeline.data_types import (
    Directory,
)

from pyimcom.config import Config, Settings
from pyimcom.coadd import Block
from pyimcom.analysis import OutImage

import metadetect_driver


class MetadetectStage(PipelineStage):
    """ """

    name = "metadetect"
    inputs = [
        # ("imcom_config", JSONFile),
        ("imcom_images", Directory),
    ]
    outputs = [("shear_catalog", Directory)]
    # outputs = [("shear_catalog", ParquetFile)]

    config_options = {
        "driver": metadetect_driver.defaults.DRIVER_DEFAULTS,
        "metadetect": metadetect_driver.defaults.METADETECT_DEFAULTS,
    }

    def run(self):
        config = self.config
        print("metadetect stage configuration :", config)

        # FIXME Not the best way to handle this...
        _driver_config = self.config_options["driver"] | config["driver"]
        _metadetect_config = self.config_options["metadetect"] | config["metadetect"]

        input_images = self.get_input("imcom_images")
        print(f"metadetect_stage reading from {input_images}")
        outimages = [OutImage(input_image) for input_image in input_images]
        results = metadetect_driver.run_metadetect(
            outimages, config=_driver_config, metadetect_config=_metadetect_config,
        )
        # results = pa.concat_tables(results.values())

        _output = self.get_output("shear_catalog")
        output = Path(_output)
        print(f"metadetect_stage writing to {output}")

        def _write_catalogs(catalogs, base_dir, block_tag):
            output_path = Path(base_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            print(f"Writing catalogs to {output_path}")

            # Ensure that all catalogs have the same schema
            schema = None
            for shear_type, catalog in catalogs.items():
                if schema is None:
                    schema = catalog.schema
                else:
                    _schema = catalog.schema
                    assert schema == _schema
            print(f"Catalog schema is {schema}")

            # TODO update output file naming
            shear_types = catalogs.keys()
            parquet_writers = {}
            for shear_type in shear_types:
                output_file = output_path / f"catalog_{block_tag}_{shear_type}.parquet"
                print(f"Opening parquet writer to {output_file}")
                parquet_writers[shear_type] = pq.ParquetWriter(
                    output_file, schema=_schema
                )

            for shear_type in shear_types:
                print(f"Writing {shear_type} catalog")
                parquet_writers[shear_type].write(catalogs[shear_type])

            for shear_type in shear_types:
                print(f"Closing parquet writer to {output_file}")
                parquet_writers[shear_type].close()

            print("Writing finished")

        _write_catalogs(results, output, block_tag)

        return 0


if __name__ == "__main__":
    cls = PipelineStage.main()
