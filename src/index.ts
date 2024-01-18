import {
  CopyObjectCommand,
  GetObjectCommand,
  GetObjectCommandOutput,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
} from "@aws-sdk/client-s3";
import { Logger, createLogger } from "@lickd/logger";
import { createWriteStream, truncateSync, unlinkSync } from "fs";

export {
  GetObjectCommandOutput,
  HeadObjectCommandOutput,
  S3Client,
  S3ServiceException,
} from "@aws-sdk/client-s3";

export class S3 {
  private logger: Logger;

  private static ONE_MB = 1024 * 1024;

  constructor(
    private s3: S3Client,
    logger?: Logger,
  ) {
    this.logger = logger || createLogger();
  }

  async headObject(
    bucket: string,
    key: string,
  ): Promise<HeadObjectCommandOutput> {
    this.logger.info(`retrieving head of '${key}' in bucket '${bucket}'`);

    try {
      const response = await this.s3.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      this.logger.info(
        `successfully retrieved head of '${key}' in bucket '${bucket}'`,
      );

      return response;
    } catch (e) {
      throw this.handleError(
        e,
        `failed to retrieve head of key '${key}' in bucket '${bucket}'`,
      );
    }
  }

  async getObject(
    bucket: string,
    key: string,
  ): Promise<GetObjectCommandOutput> {
    this.logger.info(`getting key '${key}' from bucket '${bucket}'`);

    try {
      const response = await this.s3.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      this.logger.info(`successfully got key '${key}' from bucket '${bucket}'`);

      return response;
    } catch (e) {
      throw this.handleError(
        e,
        `failed to get key '${key}' from bucket '${bucket}'`,
      );
    }
  }

  async getObjectString(bucket: string, key: string): Promise<string> {
    const object = await this.getObject(bucket, key);

    if (!object.Body || !object.ContentLength) {
      throw new Error("object body was undefined");
    }

    return object.Body.transformToString();
  }

  async getByteArray(bucket: string, key: string): Promise<Uint8Array> {
    const object = await this.getObject(bucket, key);

    if (!object.Body || !object.ContentLength) {
      throw new Error("object body was undefined");
    }

    return object.Body.transformToByteArray();
  }

  async downloadObject(
    bucket: string,
    key: string,
    filename: string,
  ): Promise<void> {
    this.logger.info(
      `downloading key '${key}' from bucket '${bucket}' to '${filename}'`,
    );

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

    const writeStream = createWriteStream(filename).on("error", (e) => {
      this.logger.error({ s3: e });
      throw e;
    });

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

      this.logger.info(
        `successfully downloaded key '${key}' from bucket '${bucket}' to '${filename}'`,
      );
    } catch (e) {
      truncateSync(filename);
      unlinkSync(filename);

      throw this.handleError(
        e,
        `failed downloading key '${key}' from bucket '${bucket}' to '${filename}'`,
      );
    }
  }

  async updateObjectMetadata(
    bucket: string,
    key: string,
    metadata: Record<string, string>,
  ): Promise<void> {
    this.logger.info(
      `updating metadata for key '${key}' in bucket '${bucket}'`,
    );

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

      this.logger.info(
        `successfully updated metadata for key '${key}' in bucket '${bucket}'`,
      );
    } catch (e) {
      throw this.handleError(
        e,
        `failed to update metadata for key '${key}' in bucket '${bucket}'`,
      );
    }
  }

  async putObject(bucket: string, key: string, body: string): Promise<void> {
    this.logger.info(`putting key '${key}' in bucket '${bucket}'`);

    try {
      await this.s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: body,
        }),
      );

      this.logger.info(`successfully put key '${key}' in bucket '${bucket}'`);
    } catch (e) {
      throw this.handleError(
        e,
        `failed to put key '${key}' in bucket '${bucket}'`,
      );
    }
  }

  private handleError(e: unknown, message: string): S3ServiceException | Error {
    if (e instanceof S3ServiceException) {
      e.message = message;

      this.logger.error({ s3: e });

      return e;
    }

    this.logger.error(message);

    return new Error(message);
  }
}
