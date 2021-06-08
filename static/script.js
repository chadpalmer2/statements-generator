document.addEventListener("DOMContentLoaded", (e) => {
    nepool_selector = document.getElementById('nepool')
    pjm_selector = document.getElementById('pjm')
    nepool_div = document.getElementById('nepool_inputs')
    pjm_div = document.getElementById('pjm_inputs')

    nepool_selector.onclick = (e) => {
        console.log("nepool clicked")
        nepool_div.style.display = ""
        pjm_div.style.display = "none"
    }

    pjm_selector.onclick = (e) => {
        console.log("pjm clicked")
        nepool_div.style.display = "none"
        pjm_div.style.display = ""
    }

    console.log("all loaded!")
});