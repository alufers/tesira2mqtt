package tesira2mqtt

func Run() {
	MustLoadConfig()
	c := NewTTPClient()
	go c.run()

	select {}
}
