package gcsresource

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"golang.org/x/oauth2"
	oauthgoogle "golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/cheggaaa/pb.v1"
	"io"
	"net/http"
	"os"
)

//go:generate counterfeiter -o fakes/fake_gcsclient.go . GCSClient
type GCSClient interface {
	BucketObjects(bucketName string, prefix string) ([]string, error)
	ObjectGenerations(bucketName string, objectPath string) ([]int64, error)
	DownloadFile(bucketName string, objectPath string, generation int64, localPath string) error
	UploadFile(bucketName string, objectPath string, objectContentType string, localPath string, predefinedACL string, cacheControl string) (int64, error)
	URL(bucketName string, objectPath string, generation int64) (string, error)
	DeleteObject(bucketName string, objectPath string, generation int64) error
	GetBucketObjectInfo(bucketName, objectPath string) (*storage.ObjectAttrs, error)
}

type gcsclient struct {
	storageService *storage.Client
	progressOutput io.Writer
}

func NewGCSClient(
	progressOutput io.Writer,
	jsonKey string,
) (GCSClient, error) {
	var err error
	var storageClient *http.Client
	var userAgent = "gcs-resource/0.0.1"

	//To provide a custom HTTP client, use option.WithHTTPClient. If you are
	// using google.golang.org/api/googleapis/transport.APIKey, use option.WithAPIKey with NewService instead.
	//TODO try option.WithAPIKey
	if jsonKey != "" {
		storageJwtConf, err := oauthgoogle.JWTConfigFromJSON([]byte(jsonKey), storage.ScopeFullControl)
		if err != nil {
			return &gcsclient{}, err
		}
		storageClient = storageJwtConf.Client(oauth2.NoContext)
	} else {
		storageClient, err = oauthgoogle.DefaultClient(oauth2.NoContext, storage.ScopeFullControl)
		if err != nil {
			return &gcsclient{}, err
		}
	}

	ctx := context.Background()
	storageService, err := storage.NewClient(ctx, option.WithHTTPClient(storageClient), option.WithUserAgent(userAgent))
	if err != nil {
		return &gcsclient{}, err
	}

	return &gcsclient{
		storageService: storageService,
		progressOutput: progressOutput,
	}, nil
}

func (gcsclient *gcsclient) BucketObjects(bucketName string, prefix string) ([]string, error) {
	bucketObjects, err := gcsclient.getBucketObjects(bucketName, prefix)
	if err != nil {
		return []string{}, err
	}

	return bucketObjects, nil
}

func (gcsclient *gcsclient) ObjectGenerations(bucketName string, objectPath string) ([]int64, error) {
	isBucketVersioned, err := gcsclient.getBucketVersioning(bucketName)
	if err != nil {
		return []int64{}, err
	}

	if !isBucketVersioned {
		return []int64{}, errors.New("bucket is not versioned")
	}
	objectGenerations, err := gcsclient.getObjectGenerations(bucketName, objectPath)

	if err != nil {
		return []int64{}, err
	}

	return objectGenerations, nil
}

func (gcsclient *gcsclient) DownloadFile(bucketName string, objectPath string, generation int64, localPath string) error {
	isBucketVersioned, err := gcsclient.getBucketVersioning(bucketName)
	if err != nil {
		return err
	}

	if !isBucketVersioned && generation != 0 {
		return errors.New("bucket is not versioned")
	}

	//getCall := gcsclient.storageService.Objects.Get(bucketName, objectPath)
	//if generation != 0 {
	//	getCall = getCall.Generation(generation)
	//}
	//
	//object, err := getCall.Do()
	//if err != nil {
	//	return err
	//}
	//
	//localFile, err := os.Create(localPath)
	//if err != nil {
	//	return err
	//}
	//defer localFile.Close()
	//
	//progress := gcsclient.newProgressBar(int64(object.Size))
	//progress.Start()
	//defer progress.Finish()
	//
	//response, err := getCall.Download()
	//if err != nil {
	//	return err
	//}
	//defer response.Body.Close()
	//
	//reader := progress.NewProxyReader(response.Body)
	//_, err = io.Copy(localFile, reader)
	//if err != nil {
	//	return err
	//}

	localFile, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer localFile.Close()
	//TODO PROGRESS BAR?
	//progress := gcsclient.newProgressBar(int64(object.Size))
	//progress.Start()
	//defer progress.Finish()
	//
	ctx := context.Background()
	objectHandle := gcsclient.storageService.Bucket(bucketName).Object(objectPath)
	if generation != 0{
		objectHandle = objectHandle.Generation(generation)
	}
	rc, err := objectHandle.NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(localFile, rc)
	if err != nil {
		return err
	}

	return nil
}

func (gcsclient *gcsclient) UploadFile(bucketName string, objectPath string, objectContentType string, localPath string, predefinedACL string, cacheControl string) (int64, error) {
	isBucketVersioned, err := gcsclient.getBucketVersioning(bucketName)
	if err != nil {
		return 0, err
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return 0, err
	}

	localFile, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer localFile.Close()

	progress := gcsclient.newProgressBar(stat.Size())
	progress.Start()
	defer progress.Finish()

	ctx := context.Background()
	wc := gcsclient.storageService.Bucket(bucketName).Object(objectPath).NewWriter(ctx)
	if _, err = io.Copy(wc, localFile); err != nil {
		return 0, err
	}

	if err := wc.Close(); err != nil {
		return 0, err
	}

	if predefinedACL != "" {
		//TODO set ACL
		println("set a predefined acl")
		//gcsclient.storageService.Bucket(bucketName).Object(objectPath).ACL().Set(ctx)
	}

	if cacheControl != "" {
		//TODO cache control
		println("set cache control")
	}
	if objectContentType != "" {
		ctx = context.Background()

		attrs := storage.ObjectAttrsToUpdate{
			//EventBasedHold     optional.Bool
			//TemporaryHold      optional.Bool
			ContentType: objectContentType,
			//ContentLanguage    optional.String
			//ContentEncoding    optional.String
			//ContentDisposition optional.String
			//CacheControl:       optional.String
			//Metadata           map[string]string // set to map[string]string{} to delete
			//ACL                []ACLRule

			//// If not empty, applies a predefined set of access controls. ACL must be nil.
			//// See https://cloud.google.com/storage/docs/json_api/v1/objects/patch.
			//PredefinedACL string
		}
		_, err = gcsclient.storageService.Bucket(bucketName).Object(objectPath).Update(ctx, attrs)
		if err != nil {
			return 0, nil
		}
		//TODO media options
		//var mediaOptions []googleapi.MediaOption
		//if objectContentType != "" {
		//	mediaOptions = append(mediaOptions, googleapi.ContentType(objectContentType))
		//}
	}

	//object := &storage.Object{
	//	Name:         objectPath,
	//	ContentType:  objectContentType,
	//	CacheControl: cacheControl,
	//}
	//
	//var mediaOptions []googleapi.MediaOption
	//if objectContentType != "" {
	//	mediaOptions = append(mediaOptions, googleapi.ContentType(objectContentType))
	//}
	//
	//insertCall := gcsclient.storageService.Objects.Insert(bucketName, object).Media(progress.NewProxyReader(localFile), mediaOptions...)
	//if predefinedACL != "" {
	//	insertCall = insertCall.PredefinedAcl(predefinedACL)
	//}
	//
	//uploadedObject, err := insertCall.Do()
	//if err != nil {
	//	return 0, err
	//}
	//
	if isBucketVersioned {
		attrs, err := gcsclient.GetBucketObjectInfo(bucketName, objectPath)
		if err != nil {
			return 0, err
		}
		return attrs.Generation, nil
	}
	return 0, nil
}

func (gcsclient *gcsclient) URL(bucketName string, objectPath string, generation int64) (string, error) {
	ctx := context.Background()
	objectHandle := gcsclient.storageService.Bucket(bucketName).Object(objectPath)
	if generation != 0{
		objectHandle = objectHandle.Generation(generation)
	}
	attrs, err := objectHandle.Attrs(ctx)
	if err != nil {
		return "", err
	}

	var url string
	if generation != 0 {
		url = fmt.Sprintf("gs://%s/%s#%d", bucketName, objectPath, attrs.Generation)
	} else {
		url = fmt.Sprintf("gs://%s/%s", bucketName, objectPath)
	}

	return url, nil
}

func (gcsclient *gcsclient) DeleteObject(bucketName string, objectPath string, generation int64) error {
	//deleteCall := gcsclient.storageService.Objects.Delete(bucketName, objectPath)
	//if generation != 0 {
	//	deleteCall = deleteCall.Generation(generation)
	//}
	//
	//err := deleteCall.Do()
	//if err != nil {
	//	return err
	//}
	//
	//return nil
	var err error
	ctx := context.Background()
	if generation != 0 {
		err = gcsclient.storageService.Bucket(bucketName).Object(objectPath).Generation(generation).Delete(ctx)
	}else {
		err = gcsclient.storageService.Bucket(bucketName).Object(objectPath).Delete(ctx)
	}
	if err != nil {
		return err
	}
	return nil
}

func (gcsclient *gcsclient) GetBucketObjectInfo(bucketName, objectPath string) (*storage.ObjectAttrs, error) {
	//getCall := gcsclient.storageService.Objects.Get(bucketName, objectPath)
	//object, err := getCall.Do()
	//if err != nil {
	//	return nil, err
	//}
	//
	//return object, nil

	ctx := context.Background()
	attrs, err := gcsclient.storageService.Bucket(bucketName).Object(objectPath).Attrs(ctx)

	if err != nil {
		return nil, err
	}
	return attrs, nil
}

func (gcsclient *gcsclient) getBucketObjects(bucketName string, prefix string) ([]string, error) {
	//var bucketObjects []string
	//
	//pageToken := ""
	//for {
	//	listCall := gcsclient.storageService.Objects.List(bucketName)
	//	listCall = listCall.PageToken(pageToken)
	//	listCall = listCall.Prefix(prefix)
	//	listCall = listCall.Versions(false)
	//
	//	objects, err := listCall.Do()
	//	if err != nil {
	//		return bucketObjects, err
	//	}
	//
	//	for _, object := range objects.Items {
	//		bucketObjects = append(bucketObjects, object.Name)
	//	}
	//
	//	if objects.NextPageToken != "" {
	//		pageToken = objects.NextPageToken
	//	} else {
	//		break
	//	}
	//}
	//
	//return bucketObjects, nil

	var bucketObjects []string
		ctx := context.Background()
		pageToken := ""
		query := &storage.Query{
			Delimiter: pageToken,
			Prefix:    prefix,
			Versions:  false,
		}
		objectIterator := gcsclient.storageService.Bucket(bucketName).Objects(ctx, query)
		for {
			object, err := objectIterator.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			bucketObjects = append(bucketObjects, object.Name)
		}

	return bucketObjects, nil
}

func (gcsclient *gcsclient) getBucketVersioning(bucketName string) (bool, error) {
	ctx := context.Background()
	bucket, err := gcsclient.storageService.Bucket(bucketName).Attrs(ctx)
	if err != nil {
		return false, err
	}

	return bucket.VersioningEnabled, nil
}

func (gcsclient *gcsclient) getObjectGenerations(bucketName string, objectPath string) ([]int64, error) {
	//var objectGenerations []int64
	//
	//pageToken := ""
	//for {
	//	listCall := gcsclient.storageService.Bucket(bucketName).Objects()
	//	listCall = listCall.PageToken(pageToken)
	//	listCall = listCall.Prefix(objectPath)
	//	listCall = listCall.Versions(true)
	//
	//	objects, err := listCall.Do()
	//	if err != nil {
	//		return objectGenerations, err
	//	}
	//
	//	for _, object := range objects.Items {
	//		if object.Name == objectPath {
	//			objectGenerations = append(objectGenerations, object.Generation)
	//		}
	//	}
	//
	//	if objects.NextPageToken != "" {
	//		pageToken = objects.NextPageToken
	//	} else {
	//		break
	//	}
	//}
	//
	//return objectGenerations, nil

	var objectGenerations []int64
	ctx := context.Background()
	pageToken := ""
	query := &storage.Query{
		Delimiter: pageToken,
		Prefix:    objectPath,
		Versions:  true,
	}
	objectIterator := gcsclient.storageService.Bucket(bucketName).Objects(ctx, query)
	for {
		object, err := objectIterator.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if object.Name == objectPath {
			objectGenerations = append(objectGenerations, object.Generation)
		}
		objectIterator.PageInfo()
	}

	return objectGenerations, nil
}

func (gcsclient *gcsclient) newProgressBar(total int64) *pb.ProgressBar {
	progress := pb.New64(total)

	progress.Output = gcsclient.progressOutput
	progress.ShowSpeed = true
	progress.Units = pb.U_BYTES
	progress.NotPrint = true

	return progress.SetWidth(80)
}
