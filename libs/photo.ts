
import { getS3, s3Delete, s3Exist } from 'douhub-helper-service';
import sharp from "sharp";
import stream from "stream";
import { _track } from 'douhub-helper-util';

export const processPhotoS3SNSForCreate = async (record: Record<string, any>) => {

    const srcBucket = record.s3.bucket.name;
    const srcKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    if (_track) console.log({ srcBucket, srcKey });

    // Sanity check: validate that source and destination are different buckets.
    if (srcKey.indexOf('.resized.') > 0) return;

    const imageType = srcKey.split('.')[srcKey.split('.').length - 1];

    if (imageType.toLowerCase() !== 'gif' &&
        imageType.toLowerCase() !== 'jpg' &&
        imageType.toLowerCase() !== 'jpeg' &&
        imageType.toLowerCase() !== 'png') {
        return;
    }

    const sizes = [120, 240, 480, 960, 1440];

    const ps = [];
    for (var i = 0; i < sizes.length; i++) {
        ps.push(processPhoto(srcBucket, srcKey, sizes[i]));
    }
    await Promise.all(ps);
};

export const processPhoto = async (bucketName: string, fileName: string, size: number) => {
    return await creatWebpPhoto(bucketName, await resizePhoto(bucketName, fileName, size));
};


export const resizePhoto = async (bucketName: string, fileName: string, size: number, region?: string) => {

    const s3 = getS3(region);

    const sourceStream = s3.getObject({
        Bucket: bucketName,
        Key: fileName
    }).createReadStream();

    const targetStream = new stream.PassThrough();
    const targetKey = `${fileName}.resized.${size}.jpg`;
    const upload = s3.upload({
        Key: targetKey,
        Bucket: bucketName,
        Body: targetStream,
    }).promise();
    sourceStream.pipe(sharp().resize(size)).pipe(targetStream);
    await upload;
    return targetKey;
};


export const creatWebpPhoto = async (bucketName: string, fileName: string, region?: string) => {

    const s3 = getS3(region);
    const sourceStream = s3.getObject({
        Bucket: bucketName,
        Key: fileName
    }).createReadStream();

    const targetStream = new stream.PassThrough();
    const targetKey = `${fileName}.webp`;
    const upload = s3.upload({
        Key: targetKey,
        Bucket: bucketName,
        Body: targetStream,
    }).promise();
    sourceStream.pipe(sharp().webp()).pipe(targetStream);
    await upload;
    return targetKey;
};


export const processPhotoS3SNSForDelete = async (record: Record<string, any>) => {

    const srcBucket = record.s3.bucket.name;
    const srcKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    if (_track) console.log({ srcBucket, srcKey });

    // Sanity check: validate that source and destination are different buckets.
    if (srcKey.indexOf('.resized.') > 0) return;

    const imageType = srcKey.split('.')[srcKey.split('.').length - 1];

    if (imageType.toLowerCase() !== 'gif' &&
        imageType.toLowerCase() !== 'jpg' &&
        imageType.toLowerCase() !== 'jpeg' &&
        imageType.toLowerCase() !== 'png') {
        return;
    }
    const sizes = [120, 240, 480, 960, 1440];

    const ps = [];
    for (var i = 0; i < sizes.length; i++) {
        ps.push(checkAndDelete(srcBucket, `${srcKey}.resized.${sizes[i]}.jpg`));
        ps.push(checkAndDelete(srcBucket, `${srcKey}.resized.${sizes[i]}.jpg.webp`));
    }
    await Promise.all(ps);
}

const checkAndDelete = async (bucketName: string, fileName: string, region?: string) => {

    if (await s3Exist(bucketName, fileName, region)) {
        await s3Delete(bucketName,fileName,region);
    }
}