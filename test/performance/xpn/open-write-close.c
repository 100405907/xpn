#include "all_system.h"
#include "xpn.h"
#include <sys/time.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>

// void *startstop(void *threadid){
// 	int ret;
// 	long tid = (long)threadid;
// 	//xpn-init
// 	ret = xpn_init();
// 	printf("%d = xpn_init() %ld\n", ret, tid);
// 	sleep(10);
// 	// xpn-destroy
// 	ret = xpn_destroy();
// 	printf("%d = xpn_destroy() %ld\n", ret, tid);
// 	pthread_exit(NULL);
// }

void startstop(int pid){
	int ret;
	//xpn-init
	ret = xpn_init();
	printf("%d = xpn_init() %d\n", ret, pid);
	sleep(10);
	// xpn-destroy
	ret = xpn_destroy();
	printf("%d = xpn_destroy() %d\n", ret, pid);
	pthread_exit(NULL);
}

int main ( int argc, char *argv[] )
{
	int    ret, rc, status;
	pid_t pid;
	pthread_t threads[5];

        if (argc < 3)
	{
	    printf("\n") ;
	    printf(" Usage: %s <full path> <megabytes to write>\n", argv[0]) ;
	    printf("\n") ;
	    printf(" Example:") ;
	    printf(" env XPN_CONF=./xpn.conf XPN_DNS=/shared/tcp_server.dns %s /P1/test_1 2\n", argv[0]);
	    printf("\n") ;
	    return -1 ;
	}	

	// for (int i = 0; i < 5; i++) {
    //     pid = fork();
    //     if (pid < 0) {
    //         // Error al crear el proceso hijo
    //         perror("fork");
    //         exit(EXIT_FAILURE);
    //     } else if (pid == 0) {
    //         // Proceso hijo
    //         startstop(i);
    //         exit(EXIT_SUCCESS);
    //     } else {
    //         // Proceso padre
    //         printf("Created process with ID: %d\n", pid);
    //     }
    // }
    // // Esperamos a que todos los procesos hijos terminen
    // for (int i = 0; i < 5; i++) {
    //     wait(&status);
    // }

	for (long i = 0; i < 5; i++){
		rc = pthread_create(&threads[i], NULL, startstop, (void *)i);
	}

	for (long i = 0; i < 5; i++){
		pthread_join(threads[i], NULL);
	}

	pthread_exit(NULL);

	// for (int i = 0; i < 5; i++){
	// 	// xpn-init
	// 	ret = xpn_init();
	// 	printf("%d = xpn_init()\n", ret);
	// 	if (ret < 0) {
	// 		return -1;
	// 	}

	// 	// xpn-destroy
	// 	ret = xpn_destroy();
	// 	printf("%d = xpn_destroy()\n", ret);
	// 	if (ret < 0) {
	// 		return -1;
	// 	}
	// }

	return 0;
}

