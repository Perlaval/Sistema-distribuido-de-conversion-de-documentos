const formData = new FormData();

for (const file of files) {
    formData.append("files", file);
}

const response = await fetch("/upload", {
    method: "POST",
    body: formData
});

const trabajos = await response.json();

for (const trabajo of trabajos) {
    createJobCard(trabajo.filename, trabajo.job_id);
    pollStatus(trabajo.job_id);
}
