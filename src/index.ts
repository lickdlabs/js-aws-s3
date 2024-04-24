import * as Sdk from "@aws-sdk/client-s3";
import { ConsoleLogger, ILogger } from "@lickd/logger";
import { ReadStream, WriteStream } from "fs";

export {
  GetObjectCommandOutput,
  HeadObjectCommandOutput,
  S3Client,
  S3ServiceException,
  StorageClass
} from "@aws-sdk/client-s3";

export class S3 {
  private logger: ILogger;

  private storageClass: Sdk.StorageClass;

  private static ONE_MB = 1024 * 1024;

  private static FIVE_MB = S3.ONE_MB * 5;

  constructor(
    private s3: Sdk.S3Client,
    logger?: ILogger,
    storageClass?: Sdk.StorageClass,
  ) {
    this.logger = logger || new ConsoleLogger();
    this.storageClass = storageClass || Sdk.StorageClass.STANDARD;
  }

  async headObject(
    bucket: string,
    key: string,
  ): Promise<Sdk.HeadObjectCommandOutput> {
    this.logger.info("retrieving head of object", { bucket, key });

    try {
      const response = await this.s3.send(
        new Sdk.HeadObjectCommand({
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
      throw this.generateError(
        error,
        `failed to retrieve head of object '${key}' from '${bucket}'`,
      );
    }
  }

  async getObject(
    bucket: string,
    key: string,
  ): Promise<Sdk.GetObjectCommandOutput> {
    this.logger.info("getting object", { bucket, key });

    try {
      const response = await this.s3.send(
        new Sdk.GetObjectCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      this.logger.info("successfully got object", { bucket, key });

      return response;
    } catch (error) {
      throw this.generateError(
        error,
        `failed to get object '${key}' from '${bucket}'`,
      );
    }
  }

  async getObjectString(bucket: string, key: string): Promise<string> {
    const object = await this.getObject(bucket, key);

    if (!object.Body || !object.ContentLength) {
      throw this.generateError(new Error(), "object body was undefined");
    }

    return object.Body.transformToString();
  }

  async getByteArray(bucket: string, key: string): Promise<Uint8Array> {
    const object = await this.getObject(bucket, key);

    if (!object.Body || !object.ContentLength) {
      throw this.generateError(new Error(), "object body was undefined");
    }

    return object.Body.transformToByteArray();
  }

  async downloadObject(
    bucket: string,
    key: string,
    stream: WriteStream,
  ): Promise<void> {
    this.logger.info("downloading key to file", {
      bucket,
      key,
      file: stream.path.toString(),
    });

    const isComplete = (end: number, length: number) => end === length - 1;

    const getObjectRange = (start: number, end: number) => {
      const command = new Sdk.GetObjectCommand({
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

        stream.write(await Body?.transformToByteArray());

        rangeAndLength = getRangeAndLength(ContentRange || "");
      }

      this.logger.info("successfully downloaded key to file", {
        bucket,
        key,
        file: stream.path.toString(),
      });
    } catch (error) {
      throw this.generateError(
        error,
        `failed to download object '${key}' from '${bucket}' to file '${stream.path.toString()}'`,
      );
    } finally {
      stream.end();
    }
  }

  async putObject(
    bucket: string,
    key: string,
    body: string,
    storageClass?: Sdk.StorageClass,
  ): Promise<void> {
    const input: Sdk.PutObjectCommandInput = {
      Bucket: bucket,
      Key: key,
      Body: body,
      StorageClass: storageClass || this.storageClass,
    };

    this.logger.info(
      `putting object '${input.Key}' in '${input.Bucket}' as '${input.StorageClass})'`,
    );

    try {
      await this.s3.send(new Sdk.PutObjectCommand(input));

      this.logger.info(
        `successfully put object '${input.Key}' in '${input.Bucket}' as '${input.StorageClass}'`,
      );
    } catch (error) {
      throw this.generateError(
        error,
        `failed to put object put object '${input.Key}' in '${input.Bucket}' as '${input.StorageClass}'`,
      );
    }
  }

  async uploadObject(
    bucket: string,
    key: string,
    stream: ReadStream,
    storageClass?: Sdk.StorageClass,
  ): Promise<void> {
    if (stream.readableHighWaterMark < S3.FIVE_MB) {
      throw new Error("stream high water mark must be 5 megabytes or higher");
    }
    
    this.logger.info("uploading file to key", {
      file: stream.path.toString(),
      bucket,
      key,
    });

    return new Promise(async (resolve) => {
      const upload = await this.createMultipartUpload(
        bucket,
        key,
        storageClass,
      );

      const promises: Promise<Sdk.UploadPartCommandOutput>[] = [];

      stream.on("data", (data) => {
        const input: Sdk.UploadPartCommandInput = {
          Bucket: bucket,
          Key: key,
          UploadId: upload.UploadId,
          PartNumber: promises.length + 1,
          Body: data,
        };

        promises.push(this.s3.send(new Sdk.UploadPartCommand(input)));
      });

      stream.on("end", async () => {
        this.logger.info(`uploading ${promises.length} parts to s3`);

        const parts = await Promise.all(promises);

        this.logger.info(
          `successfully uploaded ${promises.length} parts to s3`,
        );

        await this.completeMultipartUpload(bucket, key, upload, parts);

        this.logger.info("successfully uploaded file to key", {
          file: stream.path.toString(),
          bucket,
          key,
        });

        resolve();
      });
    });
  }

  private generateError(error: unknown, message: string) {
    this.logger.error(message);

    if (error instanceof Error) {
      error.message = message;
    } else {
      error = new Error(message);
    }

    return error;
  }

  private async createMultipartUpload(
    bucket: string,
    key: string,
    storageClass?: Sdk.StorageClass,
  ): Promise<Sdk.CreateMultipartUploadCommandOutput> {
    const input: Sdk.CreateMultipartUploadCommandInput = {
      Bucket: bucket,
      Key: key,
      StorageClass: storageClass || this.storageClass,
    };

    this.logger.info(
      `creating multipart upload for object '${input.Key}' in '${input.Bucket}' as '${input.StorageClass})'`,
    );

    const response = await this.s3.send(
      new Sdk.CreateMultipartUploadCommand(input),
    );

    if (!response.UploadId) {
      throw new Error(
        `could not create multipart upload for object '${input.Key}' in '${input.Bucket}' as '${input.StorageClass})'`,
      );
    }

    this.logger.info(
      `successfully created multipart upload for object '${input.Key}' in '${input.Bucket}' as '${input.StorageClass})'`,
    );

    return response;
  }

  private async completeMultipartUpload(
    bucket: string,
    key: string,
    upload: Sdk.CreateMultipartUploadCommandOutput,
    parts: Sdk.UploadPartCommandOutput[],
  ): Promise<void> {
    const input: Sdk.CompleteMultipartUploadCommandInput = {
      Bucket: bucket,
      Key: key,
      UploadId: upload.UploadId,
      MultipartUpload: {
        Parts: parts.map(({ ETag }, i) => ({
          ETag,
          PartNumber: i + 1,
        })),
      },
    };

    this.logger.info(
      `completing multipart upload for object '${input.Key}' in '${input.Bucket}'`,
    );

    await this.s3.send(new Sdk.CompleteMultipartUploadCommand(input));

    this.logger.info(
      `successfully completed multipart upload for object '${input.Key}' in '${input.Bucket}'`,
    );
  }
}
