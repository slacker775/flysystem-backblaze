<?php

namespace Mhetreramesh\Flysystem;

use BackblazeB2\Client;
use BackblazeB2\File;
use GuzzleHttp\Psr7;
use InvalidArgumentException;
use League\Flysystem\Config;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\MimeTypeDetection\FinfoMimeTypeDetector;

class BackblazeAdapter implements FilesystemAdapter
{
    protected FinfoMimeTypeDetector $detector;

    public function __construct(protected Client $client, protected $bucketName, protected $bucketId = null)
    {
        $this->detector = new FinfoMimeTypeDetector();
    }

    /**
     * {@inheritdoc}
     */
    public function fileExists(string $path): bool
    {
        return $this->getClient()->fileExists(
            ['FileName'   => $path, 'BucketId' => $this->bucketId,
             'BucketName' => $this->bucketName]
        );
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * {@inheritdoc}
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $file = $this->getClient()->upload([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
            'FileName'   => $path,
            'Body'       => $contents,
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $file = $this->getClient()->upload([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
            'FileName'   => $path,
            'Body'       => $contents,
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function read(string $path): string
    {
        $file = $this->getClient()->getFile([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
            'FileName'   => $path,
        ]);
        $fileContent = $this->getClient()->download([
            'FileId' => $file->getId(),
        ]);

        return $fileContent;
    }

    /**
     * {@inheritdoc}
     */
    public function readStream(string $path)
    {
        $stream = Psr7\Utils::streamFor();
        $download = $this->getClient()->download([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
            'FileName'   => $path,
            'SaveAs'     => $stream,
        ]);
        $stream->seek(0);

        try {
            $resource = Psr7\StreamWrapper::getResource($stream);
        } catch (InvalidArgumentException $e) {
            return false;
        }

        return $download === true ? $resource : false;
    }

    /**
     * {@inheritdoc}
     */
    public function move(string $path, string $newpath, Config $config): void
    {
    }

    /**
     * {@inheritdoc}
     */
    public function copy(string $path, string $newPath, Config $config): void
    {
        $this->getClient()->upload([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
            'FileName'   => $newPath,
            'Body'       => @file_get_contents($path),
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function delete(string $path): void
    {
        $this->getClient()->deleteFile(
            ['FileName'   => $path, 'BucketId' => $this->bucketId,
             'BucketName' => $this->bucketName]
        );
    }

    /**
     * {@inheritdoc}
     */
    public function deleteDirectory(string $path): void
    {
        $this->getClient()->deleteFile(
            ['FileName'   => $path, 'BucketId' => $this->bucketId,
             'BucketName' => $this->bucketName]
        );
    }

    /**
     * {@inheritdoc}
     */
    public function createDirectory(string $path, Config $config): void
    {
        $this->getClient()->upload([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
            'FileName'   => $path,
            'Body'       => '',
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function mimeType(string $path): FileAttributes
    {
        $type = $this->detector->detectMimeTypeFromFile($path);
        return new FileAttributes($path,null,null, null, $type);
    }

    /**
     * {@inheritdoc}
     */
    public function fileSize(string $path): FileAttributes
    {
        $file = $this->getClient()->getFile(
            ['FileName'   => $path, 'BucketId' => $this->bucketId,
             'BucketName' => $this->bucketName]
        );

        return $this->getFileInfo($file);
    }

    /**
     * Get file info.
     *
     * @param $file
     *
     * @return array
     */
    protected function getFileInfo(File $file): FileAttributes
    {
        $timestamp = substr($file->getUploadTimestamp(), 0, -3);
        if($timestamp === '') {
            $timestamp = null;
        }
        return new FileAttributes(
            $file->getName(), $file->getSize(), null,
            $timestamp,
            $this->detector->detectMimeTypeFromFile($file->getName())
        );
    }

    /**
     * {@inheritdoc}
     */
    public function lastModified(string $path): FileAttributes
    {
        $file = $this->getClient()->getFile(
            ['FileName'   => $path, 'BucketId' => $this->bucketId,
             'BucketName' => $this->bucketName]
        );

        return $this->getFileInfo($file);
    }

    public function setVisibility(string $path, string $visibility): void
    {
    }

    public function visibility(string $path): FileAttributes
    {
        return new FileAttributes($path);
    }

    /**
     * {@inheritdoc}
     */
    public function listContents(string $directory = '', bool $recursive = false
    ): iterable {
        $fileObjects = $this->getClient()->listFiles([
            'BucketId'   => $this->bucketId,
            'BucketName' => $this->bucketName,
        ]);
        if ($recursive === true && $directory === '') {
            $regex = '/^.*$/';
        } elseif ($recursive === true && $directory !== '') {
            $regex = '/^' . preg_quote($directory) . '\/.*$/';
        } elseif ($recursive === false && $directory === '') {
            $regex = '/^(?!.*\\/).*$/';
        } elseif ($recursive === false && $directory !== '') {
            $regex = '/^' . preg_quote($directory) . '\/(?!.*\\/).*$/';
        } else {
            throw new InvalidArgumentException();
        }
        $fileObjects = array_filter(
            $fileObjects, function ($fileObject) use ($regex) {
            return 1 === preg_match($regex, $fileObject->getName());
        }
        );
        $normalized = array_map(function ($fileObject) {
            return $this->getFileInfo($fileObject);
        }, $fileObjects);

        return array_values($normalized);
    }

}
