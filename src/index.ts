import {
  CopyObjectCommand,
  GetObjectCommand,
  GetObjectCommandOutput,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  PutObjectCommand,
  S3Client,
  StorageClass,
} from "@aws-sdk/client-s3";
import { ConsoleLogger, ILogger } from "@lickd/logger";
import { WriteStream } from "fs";

export {
  GetObjectCommandOutput,
  HeadObjectCommandOutput,
  S3Client,
  S3ServiceException,
  StorageClass,
} from "@aws-sdk/client-s3";

export class S3 {
  private logger: ILogger;

  private static ONE_MB = 1024 * 1024;

  constructor(
    private s3: S3Client,
    logger?: ILogger,
  ) {
    this.logger = logger || new ConsoleLogger();
  }

  async headObject(
    bucket: string,
    key: string,
  ): Promise<HeadObjectCommandOutput> {
    this.logger.info("retrieving head of object", { bucket, key });

    try {
      const response = await this.s3.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      this.logger.info("successfully retrieved head of object", {
        bucket,
        key,
      });

      return response;
    } catch (error) {
      this.logger.error("failed to retrieve head of object", error, {
        bucket,
        key,
      });

      throw error;
    }
  }

  async getObject(
    bucket: string,
    key: string,
  ): Promise<GetObjectCommandOutput> {
    this.logger.info("getting object", { bucket, key });

    try {
      const response = await this.s3.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      this.logger.info("successfully got object", { bucket, key });

      return response;
    } catch (error) {
      this.logger.error("failed to get object", error, { bucket, key });

      throw error;
    }
  }

  async getObjectString(bucket: string, key: string): Promise<string> {
    const object = await this.getObject(bucket, key);

    if (!object.Body || !object.ContentLength) {
      const error = new Error("object body was undefined");

      this.logger.error(error);

      throw error;
    }

    return object.Body.transformToString();
  }

  async getByteArray(bucket: string, key: string): Promise<Uint8Array> {
    const object = await this.getObject(bucket, key);

    if (!object.Body || !object.ContentLength) {
      const error = new Error("object body was undefined");

      this.logger.error(error);

      throw error;
    }

    return object.Body.transformToByteArray();
  }

  async downloadObject(
    bucket: string,
    key: string,
    writeStream: WriteStream,
  ): Promise<void> {
    this.logger.info("downloading key to file", {
      bucket,
      key,
      file: writeStream.path.toString(),
    });

    const isComplete = (end: number, length: number) => end === length - 1;

    const getObjectRange = (start: number, end: number) => {
      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${start}-${end}`,
      });

      return this.s3.send(command);
    };

    const getRangeAndLength = (contentRange: string) => {
      const [range, length] = contentRange.split("/");
      const [start, end] = range.split("-");

      return {
        start: parseInt(start),
        end: parseInt(end),
        length: parseInt(length),
      };
    };

    let rangeAndLength = { start: -1, end: -1, length: -1 };

    try {
      while (!isComplete(rangeAndLength.end, rangeAndLength.length)) {
        const { end } = rangeAndLength;
        const nextRange = { start: end + 1, end: end + S3.ONE_MB };

        const { ContentRange, Body } = await getObjectRange(
          nextRange.start,
          nextRange.end,
        );

        writeStream.write(await Body?.transformToByteArray());

        rangeAndLength = getRangeAndLength(ContentRange || "");
      }

      this.logger.info("successfully downloaded key to file", {
        bucket,
        key,
        file: writeStream.path.toString(),
      });
    } catch (error) {
      this.logger.error("failed to download key to file", {
        bucket,
        key,
        file: writeStream.path.toString(),
      });

      throw error;
    }
  }

  async updateObjectMetadata(
    bucket: string,
    key: string,
    metadata: Record<string, string>,
  ): Promise<void> {
    this.logger.info("updating metadata of object", { bucket, key, metadata });

    try {
      await this.s3.send(
        new CopyObjectCommand({
          Bucket: bucket,
          Key: key,
          CopySource: [bucket, key].join("/"),
          MetadataDirective: "REPLACE",
          Metadata: metadata,
        }),
      );

      this.logger.info("successfully updated metadata of object", {
        bucket,
        key,
      });
    } catch (error) {
      this.logger.info("failed to update metadata of object", { bucket, key });

      throw error;
    }
  }

  async putObject(
    bucket: string,
    key: string,
    body: string,
    storageClass: StorageClass = StorageClass.STANDARD,
  ): Promise<void> {
    this.logger.info("putting object", { bucket, key });

    try {
      await this.s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: body,
          StorageClass: storageClass,
        }),
      );

      this.logger.info("successfully put object", { bucket, key });
    } catch (error) {
      this.logger.error("failed to put object", { bucket, key });

      throw error;
    }
  }
}
