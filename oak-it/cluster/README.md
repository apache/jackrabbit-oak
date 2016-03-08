# Oak Cluster Test

This project is a playground for reconstructing clustering problems with oak. It uses an embedded mongodb and brings a junit rule for creating oak cluster nodes.

## Usage JUnit-Rule of OakClusterRepository: 

```Java

    @Rule
    public OakClusterRepository oakClusterRepository = new OakClusterRepository();

    @Test
    public void shouldCreateClusterOfThree() throws Exception {
        javax.jcr.Repository one = oakClusterRepository.repository();
        javax.jcr.Repository two = oakClusterRepository.repository();  
```

## Reports

### javax.jcr.InvalidItemStateException: OakMerge0001: OakMerge0001: Failed to merge changes to the underlying store

This exceptions happens a lot while writing with several sessions across a cluster. 
See [detailed report page](reports/2REPOS_50THREADS.md)
