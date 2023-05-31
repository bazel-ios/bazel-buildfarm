  @Test
  public void executesWithoutTheExpiration()
      throws IOException, InterruptedException {
/*
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
*/

    Blob expiringBlob;
    ByteString content;
    byte[] buf = new byte[512];
    try (ByteString.Output out = ByteString.newOutput(512)) {
      for (int i = 0; i < 512; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
      content = out.toByteString();
    }
    blobs.put(expiringBlob.getDigest(), expiringBlob.getData());

    String expiringKey = fileCache.getKey(expiringBlob.getDigest(), /* isExecutable=*/ false);

    ImmutableList.Builder<String> keysBuilder = new ImmutableList.Builder<>();
    keysBuilder.add(expiringKey);
    fileCache.incrementKeys(keysBuilder.build());

    //ByteString content = ByteString.copyFromUtf8("Hello, World");
    //Blob expiringBlob = new Blob(content, DIGEST_UTIL);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(expiringBlob.getDigest()), eq(0L)))
        .thenReturn(content.newInput());

    blobs.put(expiringBlob.getDigest(), expiringBlob.getData());
    decrementReference(
        fileCache.put(expiringBlob.getDigest(), /* isExecutable=*/ false)); // expected eviction
    blobs.clear();
    decrementReference(
        fileCache.put(
            expiringBlob.getDigest(),
            /* isExecutable=*/ true)); // should be fed from storage directly, not through delegate


    Blob blob1 = new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL);
    fileCache.put(blob1);
    //fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World2"), DIGEST_UTIL));


    blobs.clear();

    //verifyZeroInteractions(onExpire);
    // assert expiration of non-executable digest
    //assertThat(storage.containsKey(expiringKey)).isTrue();
    //assertThat(Files.exists(fileCache.getPath(expiringKey))).isTrue();
  }


